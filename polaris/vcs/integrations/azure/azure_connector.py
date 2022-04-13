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
            url=repo.get('remoteUrl'),
            public=False,
            vendor='git',
            integration_type=VcsIntegrationTypes.azure.value,
            source_id=repo.get('id'),
            polling=True,
            properties=dict(
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
                self.build_url(f'git/repositories?includeAllUrls=True&$top=1{page}'),
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
        if self.access_token is not None:
            # Delete existing/registered webhook before registering new
            deleted_hook_ids = []
            for inactive_hook_id in registered_webhooks:
                if self.delete_repository_webhook(repo_source_id, inactive_hook_id):
                    logger.info(f"Deleted webhook with id {inactive_hook_id} for source repo {repo_source_id}")
                    deleted_hook_ids.append(inactive_hook_id)
                else:
                    logger.info(f"Webhook with id {inactive_hook_id} for repo {repo_source_id} could not be deleted")

            # register new hook
            github = self.get_github_client()
            repo = github.get_repo(int(repo_source_id))

            repository_webhooks_callback_url = f"{config_provider.get('GITHUB_WEBHOOKS_BASE_URL')}/repository/webhooks/{self.key}/"

            try:
                new_webhook = repo.create_hook(
                    name='web',
                    config=dict(
                        url=repository_webhooks_callback_url,
                        content_type="json",
                        insecure_ssl="0"
                    ),
                    events=self.webhook_events,
                    active=True,
                )
                active_hook_id = new_webhook.id
            except GithubException as e:
                logger.error(
                    f"Failed to register webhooks for github repository with source_id: {repo_source_id}: {e.data.get('message')} error: {e.data.get('error')}")
                raise ProcessingException(
                    f"Webhook registration failed due to: {e.data.get('message')} error: {e.data.get('errors')}")
            return dict(
                success=True,
                active_webhook=active_hook_id,
                deleted_webhooks=deleted_hook_ids,
                registered_events=self.webhook_events,
            )
        else:
            raise ProcessingException(f"Github Access Token is None")

    def delete_repository_webhook(self, repo_source_id, inactive_hook_id):
        github = self.get_github_client()
        repo = github.get_repo(int(repo_source_id))
        try:
            hook = repo.get_hook(inactive_hook_id)
            hook.delete()
            return True
        except:
            logging.info(f"Could not delete webhook {inactive_hook_id} for repo {repo_source_id}")


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
        self.access_token = connector.access_token

    def map_pull_request_info(self, pull_request):
        if pull_request.merged_at is not None:
            pr_end_date = pull_request.merged_at.strftime("%Y-%m-%d %H:%M:%S")
        elif pull_request.closed_at is not None:
            pr_end_date = pull_request.closed_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            pr_end_date = None
        return dict(
            source_id=pull_request.id,
            source_display_id=pull_request.number,
            title=pull_request.title,
            description=pull_request.body,
            source_state=pull_request.state,
            state=self.pull_request_state_mapping['merged'] if pull_request.merged_at is not None else
            self.pull_request_state_mapping[
                pull_request.state],
            source_created_at=pull_request.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            source_last_updated=pull_request.updated_at.strftime(
                "%Y-%m-%d %H:%M:%S") if pull_request.updated_at else None,
            # TODO: Figure out how to determine merge status.
            source_merge_status=None,
            source_merged_at=pull_request.merged_at.strftime("%Y-%m-%d %H:%M:%S") if pull_request.merged_at else None,
            source_closed_at=pull_request.closed_at.strftime("%Y-%m-%d %H:%M:%S") if pull_request.closed_at else None,
            end_date=pr_end_date,
            source_branch=pull_request.head.ref,
            target_branch=pull_request.base.ref,
            source_repository_source_id=pull_request.head.repo.id,
            target_repository_source_id=pull_request.base.repo.id,
            web_url=pull_request.html_url
        )

    def fetch_pull_requests_from_source(self, pull_request_source_id=None):
        if self.access_token is not None:
            github = self.connector.get_github_client()
            repo = github.get_repo(int(self.repository.source_id))

            if pull_request_source_id is None:
                # fetch all pull requests
                yield from self.fetch_all_pull_requests(repo)
            else:
                yield [self.map_pull_request_info(repo.get_pull(int(pull_request_source_id)))]

    def fetch_all_pull_requests(self, repo):
        prs_iterator = repo.get_pulls(
            state='all',
            sort='updated',
            direction='desc'
        )
        fetched_upto_last_update = False
        while prs_iterator._couldGrow() and not fetched_upto_last_update:
            pull_requests = []
            for pr in prs_iterator._fetchNextPage():
                if pr.updated_at < self.last_updated:
                    fetched_upto_last_update = True
                    break
                else:
                    pull_requests.append(self.map_pull_request_info(pr))
            yield pull_requests

    def fetch_repository_forks(self):
        if self.access_token is not None:
            github = self.connector.get_github_client()
            repo = github.get_repo(int(self.repository.source_id))
            try:
                forks_paginator = repo.get_forks()
                while forks_paginator._couldGrow():
                    repos = [
                        self.connector.map_repository_info(repo)
                        for repo in forks_paginator._fetchNextPage()
                        if not repo.archived
                    ]
                    yield repos
            except GithubException as exc:
                logger.error(f"Github Exception raised fetching forks for repo {self.name}: {str(exc)}")
                raise
