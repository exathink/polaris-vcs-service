# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch

from test.shared_fixtures import *
from polaris.vcs import commands


class TestSyncGithubRepositories:

    def it_imports_a_new_repository(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        connector_key = connectors['github']

        with patch(
                'polaris.vcs.integrations.github.GithubRepositoriesConnector.fetch_repositories_from_source') as fetch_repos:
            fetch_repos.return_value = [
                [
                    dict(
                        **repositories_common_fields
                    )
                ]
            ]
            for result in commands.sync_repositories(connector_key):
                assert result['success']
                assert len(result['repositories']) == 1

