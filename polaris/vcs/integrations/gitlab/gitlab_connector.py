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
from .gitlab_webhooks import webhook_paths
from polaris.common.enums import VcsIntegrationTypes

config_provider = get_config_provider()

logger = logging.getLogger('polaris.vcs.integrations.gitlab')


class GitlabRepositoriesConnector(GitlabConnector):

    def __init__(self, connector):
        super().__init__(connector)
        self.webhook_secret = connector.webhook_secret

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

    def register_repository_push_hook(self, repository):
        repo_source_id = repository['source_id']
        repository_push_callback_url = f"{config_provider.get('GITLAB_WEBHOOKS_BASE_URL')}" \
                                       f"{webhook_paths['repository:push']}/{self.key}"

        add_hook_url = f"{self.base_url}/projects/{repo_source_id}/hooks"

        response = requests.post(
            add_hook_url,
            headers={"Authorization": f"Bearer {self.personal_access_token}"},
            data=dict(
                id=repo_source_id,
                url=repository_push_callback_url,
                push_events=True,
                enable_ssl_verification=True,
                token=self.webhook_secret
            )
        )
        if response.ok:
            result = response.json()
            return dict(
                webhooks=dict(
                    repository_push=dict(
                        source_hook_id=result['id'],
                        created_at=result['created_at']
                    )
                )
            )
        else:
            raise ProcessingException(
                f"Failed to register repository:push webhook for repository {repository['name']} ({repo_source_id})"
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


    def map_pull_request_info(self, pull_request):
        return dict(
            source_id=pull_request['id'],
            source_display_id=pull_request['iid'],
            title=pull_request['title'],
            description=pull_request['description'],
            source_state=pull_request['state'],
            source_created_at=pull_request['created_at'],
            source_last_updated=pull_request['updated_at'],
            source_merge_status=pull_request['merge_status'],
            source_merged_at=pull_request['merged_at'],
            source_branch=pull_request['source_branch'],
            target_branch=pull_request['target_branch'],
            source_repository_source_id=pull_request['source_project_id'],
            target_repository_source_id=pull_request['target_project_id'],
            web_url=pull_request['web_url']
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
                    f"Server test failed {response.text} status: {response.status_code}\n"
                )

    def fetch_pull_requests_from_source(self):
        for pull_requests in self.fetch_pull_requests():
            yield [
                self.map_pull_request_info(pr)
                for pr in pull_requests
            ]
