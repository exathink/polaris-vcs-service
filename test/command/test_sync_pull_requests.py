# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from unittest.mock import patch

from test.shared_fixtures import *
from polaris.vcs import commands
from polaris.vcs.db import api


class TestSyncGitlabPullRequests:

    def it_fetches_new_pull_requests_from_gitlab_created_in_last_1_day(self, setup_sync_repos):
        # TODO: Create gitlab connector
        organization_key, connectors = setup_sync_repos
        repository_key = test_repository_key

        pull_requests = commands.sync_pull_requests(repository_key, connectors['gitlab'], created_after=datetime.utcnow()-timedelta(days=1))
        assert len(pull_requests) >= 0

    def it_inserts_newly_fetched_pull_requests_into_db(self, setup_org_repo):
        repository, organization = setup_org_repo

        with patch(
                'polaris.vcs.integrations.github.GitlabRepositoriesConnector.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        repository_id=repository.id,
                        **pull_request_common_fields
                    )
                ]
            ]
            for result in api.import_pull_requests(organization.key, repository.key, fetch_prs.return_value):
                assert result['success']