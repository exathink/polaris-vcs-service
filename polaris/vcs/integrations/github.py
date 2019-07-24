# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.integrations.github import GithubConnector
from polaris.common.enums import VcsIntegrationTypes

logger = logging.getLogger('polaris.vcs.integrations.github')


class GithubRepositoriesConnector(GithubConnector):

    def __init__(self, connector):
        super().__init__(connector)

    def map_repository_info(self, repo):
        return dict(
            name=repo.name,
            url=repo.html_url,
            public=not repo.private,
            vendor='git',
            integration_type=VcsIntegrationTypes.github.value,
            description=repo.description,
            source_id=repo.id,
            properties=dict(
                ssh_url=repo.ssh_url,
                homepage=repo.homepage,
                default_branch=repo.default_branch,
            ),
        )

    def fetch_repositories_from_source(self):
        repos_paginator = self.fetch_repositories()
        while repos_paginator._couldGrow():
            yield [
                self.map_repository_info(repo)
                for repo in repos_paginator._fetchNextPage()
            ]
