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

    def fetch_repositories(self):
        if self.personal_access_token is not None:
            pass
        else:
            raise ProcessingException("No access token found this Github Connector. Cannot continue.")

    def fetch_repositories_from_source(self):
        pass
