# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import pytest
from test.shared_fixtures import *

from polaris.vcs.db import api
from polaris.repos.db.schema import RepositoryImportState


class TestImportRepositories:

    def it_marks_repositories_for_import(self, setup_sync_repos):
        organization_key, _ = setup_sync_repos
        repository_key = test_repository_key

        api.import_repositories(organization_key, [repository_key])

        assert db.connection().execute(
            f"select import_state from repos.repositories where key='{repository_key}'"
        ).scalar() == RepositoryImportState.IMPORT_READY

    def it_returns_a_valid_response(self, setup_sync_repos):
        organization_key, _ = setup_sync_repos
        repository_key = test_repository_key

        result = api.import_repositories(organization_key, [repository_key])
        assert result['success']
        assert result['organization_key']
        assert len(result['repositories']) == 1
