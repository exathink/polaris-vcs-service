# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import requests
from datetime import datetime, timedelta
from polaris.integrations.gitlab import GitlabConnector
from polaris.utils.exceptions import ProcessingException
from polaris.utils.config import get_config_provider
from polaris.common.enums import VcsIntegrationTypes, GitlabPullRequestState

config_provider = get_config_provider()

logger = logging.getLogger('polaris.vcs.integrations.gitlab')


class GitlabRepositoriesConnector(GitlabConnector):

    def __init__(self, connector):
        super().__init__(connector)
        self.webhook_secret = connector.webhook_secret
        self.webhook_events = ['push_events', 'merge_requests_events']

    def map_repository_info(self, repo):
        return dict(
            name=repo['name'],
            url=repo['http_url_to_repo'],
            public=repo['visibility'] == 'public',
            vendor='git',
            integration_type=VcsIntegrationTypes.gitlab.value,
            description=repo['description'],
            source_id=repo['id'],
            polling=True,
            properties=dict(
                ssh_url=repo['ssh_url_to_repo'],
                homepage=repo['web_url'],
                default_branch=repo['default_branch'],
                path_with_namespace=repo['path_with_namespace']
            ),
        )

    def register_repository_webhooks(self, repo_source_id, registered_webhooks):
        # Delete the inactive hooks. Add all to deleted_hook_ids as either it is successfully deleted or \
        # we are storing some old id which is no longer present
        deleted_hook_ids = []
        for inactive_hook_id in registered_webhooks:
            if self.delete_repository_webhook(repo_source_id, inactive_hook_id):
                logger.info(f"Deleted webhook with id {inactive_hook_id} for repo {repo_source_id}")
                deleted_hook_ids.append(inactive_hook_id)
            else:
                logger.info(f"Webhook with id {inactive_hook_id} for repo {repo_source_id} could not be deleted")

        # Register new webhook now
        repository_webhooks_callback_url = f"{config_provider.get('GITLAB_WEBHOOKS_BASE_URL')}" \
                                           f"/repository/webhooks/{self.key}/"

        add_hook_url = f"{self.base_url}/projects/{repo_source_id}/hooks"

        post_data = dict(
            id=repo_source_id,
            url=repository_webhooks_callback_url,
            push_events=True,
            merge_requests_events=True,
            enable_ssl_verification=True,
            token=self.webhook_secret
        )
        for event in self.webhook_events:
            post_data[f'{event}'] = True

        response = requests.post(
            add_hook_url,
            headers={"Authorization": f"Bearer {self.personal_access_token}"},
            data=post_data
        )
        if response.ok:
            result = response.json()
            active_hook_id = result['id']
        else:
            raise ProcessingException(
                f"Webhook registration failed due to status:{response.status_code} message:{response.text}")
        return dict(
            success=True,
            active_webhook=active_hook_id,
            deleted_webhooks=deleted_hook_ids,
            registered_events=self.webhook_events,
        )

    def get_available_webhooks(self, repo_source_id):
        get_hooks_url = f"{self.base_url}/projects/{repo_source_id}/hooks"
        response = requests.get(
            get_hooks_url,
            headers={"Authorization": f"Bearer {self.personal_access_token}"}
        )
        if response.ok:
            result = response.json()
            available_hooks = []
            for hook in result:
                available_hooks.append(hook['id'])
            return available_hooks

    def delete_repository_webhook(self, repo_source_id, inactive_hook_id):
        delete_hook_url = f"{self.base_url}/projects/{repo_source_id}/hooks/{inactive_hook_id}"
        response = requests.delete(
            delete_hook_url,
            headers={"Authorization": f"Bearer {self.personal_access_token}"}
        )
        if response.ok or response.status_code == 404:
            # Case when hook was already non-existent or deleted successfully
            return True
        else:
            logger.info(
                f"Failed to delete repository webhooks for repository with source id: ({repo_source_id})"
                f'{response.status_code} {response.text}'
            )

    def fetch_repositories(self):
        fetch_repos_url = f'{self.base_url}/projects'
        while fetch_repos_url is not None:
            response = requests.get(
                fetch_repos_url,
                params=dict(membership=True),
                headers={"Authorization": f"Bearer {self.personal_access_token}"},
            )
            if response.ok:
                yield response.json()
                if 'next' in response.links:
                    fetch_repos_url = response.links['next']['url']
                else:
                    fetch_repos_url = None
            else:
                raise ProcessingException(
                    f"Server test failed {response.text} status: {response.status_code}\n"
                )

    def fetch_repositories_from_source(self):
        for repositories in self.fetch_repositories():
            yield [
                self.map_repository_info(repo)
                for repo in repositories
            ]


class PolarisGitlabRepository:

    @staticmethod
    def create(repository, connector):
        if repository.integration_type == VcsIntegrationTypes.gitlab.value:
            return GitlabRepository(repository, connector)
        else:
            raise ProcessingException(f"Unknown integration_type: {repository.integration_type}")


class GitlabRepository(PolarisGitlabRepository):

    def __init__(self, repository, connector):
        self.repository = repository
        self.source_repo_id = repository.source_id
        self.last_updated = repository.latest_pull_request_update_timestamp
        self.gitlab_connector = connector
        self.webhook_secret = self.gitlab_connector.webhook_secret
        self.base_url = f'{self.gitlab_connector.base_url}'
        self.personal_access_token = self.gitlab_connector.personal_access_token
        self.state_mapping = dict(
            opened=GitlabPullRequestState.opened.value,
            closed=GitlabPullRequestState.closed.value,
            merged=GitlabPullRequestState.merged.value,
            locked=GitlabPullRequestState.locked.value
        )

    def map_pull_request_info(self, pull_request):
        if pull_request.get('merged_at') is not None:
            pr_end_date = pull_request.get('merged_at')
        elif pull_request.get('closed_at') is not None:
            pr_end_date = pull_request.get('closed_at')
        else:
            pr_end_date = None
        return dict(
            source_id=pull_request.get('id'),
            source_display_id=pull_request.get('iid'),
            title=pull_request.get('title'),
            description=pull_request.get('description'),
            source_state=pull_request.get('state'),
            state=self.state_mapping[pull_request.get('state')],
            source_created_at=pull_request.get('created_at'),
            source_last_updated=pull_request.get('updated_at'),
            source_merge_status=pull_request.get('merge_status'),
            source_merged_at=pull_request.get('merged_at'),
            source_closed_at=pull_request.get('closed_at'),
            end_date=pr_end_date,
            source_branch=pull_request.get('source_branch'),
            target_branch=pull_request.get('target_branch'),
            source_repository_source_id=pull_request.get('source_project_id'),
            target_repository_source_id=pull_request.get('target_project_id'),
            # NOTE: In PR object from webhook we get 'url' and not 'web_url'
            web_url=pull_request.get('web_url') if pull_request.get('web_url') else pull_request.get('url')
        )

    def fetch_pull_requests(self):
        query_params = dict(limit=100)
        if self.last_updated is None:
            query_params['updated_after'] = (datetime.utcnow() - timedelta(days=93)).isoformat()
        else:
            query_params['updated_after'] = self.last_updated.isoformat()
        fetch_pull_requests_url = f'{self.base_url}/projects/{self.source_repo_id}/merge_requests'
        while fetch_pull_requests_url is not None:
            response = requests.get(
                fetch_pull_requests_url,
                params=query_params,
                headers={"Authorization": f"Bearer {self.personal_access_token}"},
            )
            if response.ok:
                yield response.json()
                if 'next' in response.links:
                    fetch_pull_requests_url = response.links['next']['url']
                else:
                    fetch_pull_requests_url = None
            else:
                raise ProcessingException(
                    f"Fetch pull requests failed {response.text} status: {response.status_code}\n"
                )

    def get_pull_request(self, source_id):
        fetch_pull_requests_url = f'{self.base_url}/projects/{self.source_repo_id}/merge_requests/{source_id}'

        response = requests.get(
            fetch_pull_requests_url,
            headers={"Authorization": f"Bearer {self.personal_access_token}"},
        )
        if response.ok:
            return response.json()
        else:
            raise ProcessingException(
                f"Fetch pull requests failed {response.text} status: {response.status_code}\n"
            )

    def fetch_pull_requests_from_source(self, source_id=None):
        if source_id is None:
            for pull_requests in self.fetch_pull_requests():
                yield [
                    self.map_pull_request_info(pr)
                    for pr in pull_requests
                ]
        else:
            yield [
                self.map_pull_request_info(self.get_pull_request(source_id))
            ]
