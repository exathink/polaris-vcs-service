# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
from test.shared_fixtures import *
from polaris.common.enums import VcsIntegrationTypes
from polaris.utils.collections import dict_merge
from polaris.vcs.db import api







class TestSyncGithubRepositories:

    def it_imports_a_new_repository(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        connector_key = connectors['github']

        source_repos = [
            dict(
                **repositories_common_fields
            )
        ]

        result = api.sync_repositories(organization_key, connector_key, source_repos)
        assert result['success']
        assert len(result['repositories']) == 1
        assert result['repositories'][0]['is_new']

        #  Note that the setup process already creates one repo under this connector key. Thats why we have one
        # extra here.
        assert db.connection().execute(f"select count(id) from repos.repositories "
                                       f"where connector_key='{connector_key}'").scalar() == 2

    def it_is_idempotent(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        connector_key = connectors['github']

        source_repos = [
            dict(
                **repositories_common_fields
            )
        ]

        # import once
        api.sync_repositories(organization_key, connector_key,  source_repos)

        # import again
        result = api.sync_repositories(organization_key, connector_key, source_repos)
        assert result['success']
        assert len(result['repositories']) == 1
        assert not result['repositories'][0]['is_new']
        #  Note that the setup process already creates one repo under this connector key. Thats why we have one
        # extra here.
        assert db.connection().execute(f"select count(id) from repos.repositories "
                                       f"where connector_key='{connector_key}'").scalar() == 2


    def it_updates_existing_repository_records(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        connector_key = connectors['github']

        source_repos = [
            dict(
                **repositories_common_fields
            )
        ]

        # import once
        api.sync_repositories(organization_key, connector_key, source_repos)

        source_repos = [
            dict(
                **dict_merge(
                    repositories_common_fields,
                    dict(
                        url='https://baz.com'
                    )
                )
            )
        ]

        # import again
        result = api.sync_repositories(organization_key, connector_key, source_repos)
        assert result['success']
        assert db.connection().execute(f"select count(id) from repos.repositories "
                                       f"where connector_key='{connector_key}' and  url='https://baz.com'").scalar() == 1

    def it_processes_a_mix_of_new_and_existing_records(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        connector_key = connectors['github']

        source_repos = [
            dict(
                **repositories_common_fields
            )
        ]

        # import once
        api.sync_repositories(organization_key, connector_key, source_repos)

        source_repos = [
            dict(
                **dict_merge(
                    repositories_common_fields,
                    dict(
                        url='https://baz.com'
                    )
                )
            ),
            dict(
                **dict_merge(
                    repositories_common_fields,
                    dict(
                        name='another new repo',
                        url='https://ugh.io',
                        source_id='10003'
                    )
                )
            ),

        ]

        # import again
        result = api.sync_repositories(organization_key, connector_key,  source_repos)
        assert result['success']
        assert len(result['repositories']) == 2
        assert {repo['is_new'] for repo in result['repositories']} == {True, False}
        #  Note that the setup process already creates one repo under this connector key. Thats why we have one
        # extra here.
        assert db.connection().execute(f"select count(id) from repos.repositories "
                                       f"where connector_key='{connector_key}'").scalar() == 3