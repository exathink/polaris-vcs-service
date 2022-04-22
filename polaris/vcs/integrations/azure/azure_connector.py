# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import requests

from datetime import datetime, timedelta

from polaris.utils.config import get_config_provider
from polaris.integrations.azure import AzureConnector
from polaris.utils.exceptions import ProcessingException
from polaris.common.enums import VcsIntegrationTypes

config_provider = get_config_provider()
logger = logging.getLogger('polaris.vcs.integrations.azure')


class AzureRepositoriesConnector(AzureConnector):

    def __init__(self, connector):
        super().__init__(connector)
        self.webhook_events = [
                'git.push',
                'git.pullrequest.created',
                'git.pullrequest.merged'
            ]

    def map_repository_info(self, repo):
        return dict(
            name=repo.get('name') if not repo.get('isFork') else f"<- {repo.name}",
            url=repo.get('webUrl'),
            public=False,
            vendor='git',
            integration_type=VcsIntegrationTypes.azure.value,
            source_id=repo.get('id'),
            polling=True,
            properties=dict(
                remote_url=repo.get('remoteUrl'),
                ssh_url=repo.get('sshUrl'),
                default_branch=repo.get('defaultBranch', "").replace("refs/heads/", ""),
                name=repo.get('name'),
                fork=repo.get('isFork', False),
                project=repo.get('project')
            ),
        )

    def fetch_repositories(self, continuation_token=None):
        if self.personal_access_token is not None:
            page = f"&continuationToken={continuation_token}" if continuation_token else ""
            response = requests.get(
                self.build_org_url(f'git/repositories?includeAllUrls=True{page}'),
                headers=self.get_standard_headers()
            )
            if response.status_code == 200:
                body = response.json()
                return dict(
                    count=body.get('count'),
                    repos=body.get('value'),
                    continuation_token=response.headers.get('x-ms-continuation-token')
                )
            else:
                raise ProcessingException("Exception on fetching repos")
        else:
            raise ProcessingException("No access token found for this Azure Connector. Cannot continue.")

    def fetch_repositories_from_source(self):
        logger.info(
            f'Refresh Repositories: Fetching repositories for connector {self.name} in organization {self.organization_key}')
        response = self.fetch_repositories()
        count = 0
        while True:
            repos = [
                self.map_repository_info(repo)
                for repo in response.get('repos')
            ]
            count = count + response.get('count')

            yield repos
            # Note: The pagination mechanism here is untested since there
            # is no documentation for how this api is supposed to paginate its
            # result. The best that can be gleaned from reading the interwebs
            # (https://stackoverflow.com/questions/69345683/azure-devops-rest-api-top-parameter-in-powershell-script-is-not-working/69346262#69346262) is that
            # *some* apis have hardcoded page limits and if they start to paginate then
            # they return the continuation token in the response header, and reading between the lines this seems
            # to be one of them.

            # This api does not seem
            # to respect the $top parameter, so we have no way of forcing pagination to test this.
            # At some point I guess we will run into a customer who has a large enough set of repositories
            # that will trigger this and this code will either work and give us all the repos or fail and not
            # report all the available repos. We'll find out then I guess *shrug*.

            # Azure DevOps takes over from Atlassian for having the shittiest api designs at this point.
            # We are using completely different techniques to paginate repositories and pull requests
            # and this is just unbelievably amateurish.
            if response.get('continuation_token') is not None:
                response = self.fetch_repositories(response.get('continuation_token'))
            else:
                break
        logger.info(
            f"Refresh Repositories: Fetched {count} repositories in total for connector {self.name} in organization {self.organization_key}")

    def register_repository_webhooks(self, repo_source_id, registered_webhooks):
        azure_webhooks_endpoint = f'{config_provider.get("AZURE_WEBHOOKS_BASE_URL")}/repository/webhooks'
        active_webhooks = [
            self.register_azure_repository_subscription(azure_webhooks_endpoint, event_type, repo_source_id)
            for event_type in self.webhook_events
        ]
        return dict(
            success=True,
            active_webhook=active_webhooks,
            deleted_webhooks=[],
            registered_events=self.webhook_events,
        )

    def register_azure_repository_subscription(self, azure_webhooks_endpoint, event_type, repo_source_id):
        response = requests.post(
            self.build_org_url(f'hooks/subscriptions'),
            headers=self.get_post_headers(),
            json=dict(
                publisherId='tfs',
                eventType=event_type,
                consumerId='webHooks',
                consumerActionId='httpRequest',
                publisherInputs=dict(
                    repository=repo_source_id,
                ),
                consumerInputs=dict(
                    url=f"{azure_webhooks_endpoint}/{self.key}/"
                )
            )
        )
        if response.status_code == 200:
            body = response.json()
            return dict(
                event_type=event_type,
                subscription_id=body.get('id')
            )
        else:
            return None

    def delete_connector_webhooks(self, repo_source_id, inactive_hook_id):
        return True


class PolarisAzureRepository:

    @staticmethod
    def create(repository, connector):
        if repository.integration_type == VcsIntegrationTypes.azure.value:
            return AzureRepository(repository, connector)
        else:
            raise ProcessingException(f"Unknown integration type: {repository.integration_type}")


# FIXME: Hardcoded value for initial import days
INITIAL_IMPORT_DAYS = 90


class AzureRepository(PolarisAzureRepository):

    def __init__(self, repository, connector):
        self.repository = repository
        self.source_repo_id = repository.source_id
        self.last_updated = repository.latest_pull_request_update_timestamp \
            if repository.latest_pull_request_update_timestamp is not None \
            else datetime.utcnow() - timedelta(days=INITIAL_IMPORT_DAYS)
        self.connector = connector
        self.pull_request_state_map = dict(
            active='open',
            completed='closed',
            abandoned='closed',
            notSet='open'
        )

    @staticmethod
    def parse_azure_date(date_string):
        # need this because azure sends us dates with 7 digits in the microseconds
        # whereas strptime only expects 6 digit microseconds.
        return datetime.strptime(date_string[:-2], "%Y-%m-%dT%H:%M:%S.%f")

    def map_pull_request_info(self, pull_request):
        target_repository_id = pull_request['repository']['id'] if 'repository' in pull_request else None
        source_repository_id = pull_request['forkSource']['repository'][
            'id'] if 'forkSource' in pull_request else target_repository_id
        closed_date = pull_request.get('closedDate')
        return dict(
            # AZD does not have globally unique id for PRs - always need
            # to qualify by repository id for uniqueness
            source_id=pull_request['pullRequestId'],
            source_display_id=pull_request['pullRequestId'],
            title=pull_request['title'],
            description=pull_request['description'],
            source_state=pull_request['status'],
            state=self.pull_request_state_map.get(pull_request['status'], None),
            source_created_at=pull_request['creationDate'],
            # AZD does not provide a last updated  date. This sucks, but
            # we will use the last update from our side as the updated date for non closed
            # items and the closedDate for the closed items. Likely this will cause us
            # some issues down the line, but it is the closest approx I can think of
            # for a valid last updated date in the absence of a reliable one from them.
            source_last_updated=datetime.utcnow() if closed_date is None else closed_date,
            source_merge_status=pull_request['mergeStatus'],
            source_merged_at=closed_date,
            source_closed_at=closed_date,
            end_date=closed_date,
            source_branch=pull_request.get('sourceRefName', "").replace('refs/heads/', ''),
            target_branch=pull_request.get('targetRefName', "").replace('refs/heads/', ''),
            source_repository_source_id=source_repository_id,
            target_repository_source_id=target_repository_id,
            web_url=f"{self.repository.url}/pullrequest/{pull_request['pullRequestId']}"
        )

    def fetch_pull_requests(self, status, top=50, skip=0, ):
        response = requests.get(
            self.connector.build_org_url(
                f'/git/repositories/{self.source_repo_id}/pullrequests'
            ),
            headers=self.connector.get_standard_headers(),
            params={
                "searchCriteria.status": status,
                "searchCriteria.includeLinks": "true",
                "$top": top,
                "$skip": skip
            }
        )

        if response.status_code == 200:
            body = response.json()
            if body is not None:
                return dict(
                    count=body.get('count'),
                    pull_requests=body.get('value'),
                    continuation_token=response.headers.get('x-ms-continuation-token')
                )
        else:
            raise ProcessingException(
                f'Fetching Pull Requests for status {status} from source api failed: '
                f' repository {self.repository.name} in organization {self.repository.organization_key}'
                f' Response Code: {response.status_code} Response Text: {response.text}')

    def fetch_completed_pull_requests(self, days=30):
        logger.info(
            f'Fetching Completed Pull Requests:  repository {self.repository.name} in organization {self.repository.organization_key}')

        response = self.fetch_pull_requests(status='completed')
        count = 0
        search_window = datetime.utcnow() - timedelta(days=days)
        while True:
            pull_requests = [
                self.map_pull_request_info(pull_request)
                for pull_request in response.get('pull_requests')
                if self.parse_azure_date(pull_request.get('closedDate')) >= search_window
                # here we are relying on the undocumented behavior of the AZD pull requests API which
                # returns the completed pull requests in descending order of completion dates.
                # Caveat emptor as with a lot of the AZD APIs. We have no other way of limiting the
                # returned results so have to go with this for now.
            ]
            count = count + len(pull_requests)

            yield pull_requests

            if 0 < response.get('count') == len(pull_requests):
                response = self.fetch_pull_requests(skip=count, status='completed')
            else:
                break

        logger.info(
            f"{count} completed pull_requests fetched for repository {self.repository.name} in organization {self.repository.organization_key}")

    def fetch_active_pull_requests(self):
        logger.info(
            f'Fetching Active Pull Requests:  repository {self.repository.name} in organization {self.repository.organization_key}')

        response = self.fetch_pull_requests(status='active')
        count = 0
        while True:
            pull_requests = [
                self.map_pull_request_info(pull_request)
                for pull_request in response.get('pull_requests')
            ]
            count = count + len(pull_requests)

            yield pull_requests

            if len(pull_requests) > 0:
                response = self.fetch_pull_requests(skip=count, status='active')
            else:
                break

        logger.info(
            f"{count} active pull_requests fetched for repository {self.repository.name} in organization {self.repository.organization_key}")

    def fetch_pull_request(self, pull_request_source_id):
        logger.info(
            f'Fetching Pull Request {pull_request_source_id}:  repository {self.repository.name} in organization {self.repository.organization_key}')
        response = requests.get(
            self.connector.build_org_url(
                f'/git/repositories/{self.source_repo_id}/pullrequests/{pull_request_source_id}'
            ),
            headers=self.connector.get_standard_headers(),
        )
        if response.status_code == 200:
            logger.info(
                f'Fetched Pull Request {pull_request_source_id}:  repository {self.repository.name} in organization {self.repository.organization_key}'
            )
            pull_request = response.json()
            yield [
                self.map_pull_request_info(pull_request)
            ]
        else:
            raise ProcessingException(
                f'Fetching Pull Request {pull_request_source_id}  from source api failed: '
                f' repository {self.repository.name} in organization {self.repository.organization_key}'
                f' Response Code: {response.status_code} Response Text: {response.text}'
            )


    def fetch_pull_requests_from_source(self, pull_request_source_id=None):
        search_window = self.repository.properties.get('pull_requests_search_window', 30)
        if pull_request_source_id is None:
            # first fetch all the completed pull requests
            yield from self.fetch_completed_pull_requests(days=search_window)
            yield from self.fetch_active_pull_requests()
        else:
            yield from self.fetch_pull_request(pull_request_source_id)

    def fetch_repository_forks(self):
        raise NotImplementedError('This operation is not yet implemented for the Azure Connector')
