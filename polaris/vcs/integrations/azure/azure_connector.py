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
                self.build_org_url(f'git/repositories?includeAllUrls=True&$top=50{page}'),
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

            if response.get('continuation_token') is not None:
                response = self.fetch_repositories(response.get('continuation_token'))
            else:
                break
        logger.info(
            f"Refresh Repositories: Fetched {count} repositories in total for connector {self.name} in organization {self.organization_key}")

    def register_repository_webhooks(self, repo_source_id, registered_webhooks):
        return dict(
            success=False,
            active_webhook=None,
            deleted_webhooks=[],
            registered_events=[],
        )

    def delete_repository_webhook(self, repo_source_id, inactive_hook_id):
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


    def map_pull_request_info(self, pull_request):
        target_repository_id = pull_request['repository']['id'] if 'repository' in pull_request else None
        source_repository_id = pull_request['forkSource']['repository']['id'] if 'forkSource' in pull_request else target_repository_id
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


    def fetch_pull_requests(self, limit=50, continuation_token=None):
        page = f"&continuationToken={continuation_token}" if continuation_token else ""
        response = requests.get(
            self.connector.build_org_url(
                f'/git/repositories/{self.source_repo_id}/pullrequests?searchCriteria.status=all&searchCriteria.includeLinks=true&top={limit}{page}'
            ),
            headers=self.connector.get_standard_headers()
        )
        if response.status_code == 200:
            body = response.json()
            if body is not None:
                return dict(
                    count=body.get('count'),
                    pull_requests=body.get('value'),
                    continuation_token=response.headers.get('x-ms-continuation-token')
                )

    def fetch_pull_requests_from_source(self, pull_request_source_id=None):
        logger.info(
            f'Fetching Pull Requests:  repository {self.repository.name} in organization {self.repository.organization_key}')

        response = self.fetch_pull_requests()
        count = 0
        while True:
            pull_requests = [
                self.map_pull_request_info(pull_request)
                for pull_request in response.get('pull_requests')
            ]
            count = count + response.get('count')

            yield pull_requests

            if response.get('continuation_token') is not None:
                response = self.fetch_pull_requests(response.get('continuation_token'))
            else:
                break

        logger.info(
            f"Fetched {count} pull_requests in total for repository {self.repository.name} in organization {self.repository.organization_key}")

    def fetch_repository_forks(self):
        raise NotImplementedError('This operation is not yet implemented for the Azure Connector')
