# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from unittest.mock import patch

from ..shared_fixtures import *
from polaris.vcs import commands


class TestSyncGitlabPullRequests:

    def it_fetches_latest_updated_pull_requests_from_gitlab(self, setup_sync_repos_gitlab):
        organization_key, connectors = setup_sync_repos_gitlab
        repository_key = test_repository_key
        source_repo_id = '5419303'

        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                pull_requests = command_output
                assert pull_requests[0]['is_new']
        updated_pull_request = dict(
            **pull_requests_common_fields
        )
        updated_pull_request['source_last_updated'] = datetime.utcnow()
        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **updated_pull_request
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                pull_requests = command_output
                assert not pull_requests[0]['is_new']
