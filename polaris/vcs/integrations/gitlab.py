# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import requests
from polaris.integrations.gitlab import GitlabConnector
from polaris.common.enums import VcsIntegrationTypes
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.vcs.integrations.github')


class GitlabRepositoriesConnector(GitlabConnector):

    def __init__(self, connector):
        super().__init__(connector)

    def map_repository_info(self, repo):
        return dict(
            name=repo['name'],
            url=repo['http_url_to_repo'],
            public=repo['visibility'] == 'public',
            vendor='git',
            integration_type=VcsIntegrationTypes.gitlab.value,
            description=repo['description'],
            source_id=repo['id'],
            properties=dict(
                ssh_url=repo['ssh_url_to_repo'],
                homepage=repo['web_url'],
                default_branch=repo['default_branch'],
                path_with_namespace=repo['path_with_namespace']
            ),
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
