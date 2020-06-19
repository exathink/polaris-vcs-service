# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from unittest.mock import patch
from ..shared_fixtures import *
from polaris.vcs import commands, repository_factory


class TestSyncGitlabPullRequests:

    def it_fetches_latest_updated_pull_requests_from_gitlab(self, setup_sync_repos_gitlab):
        _, _ = setup_sync_repos_gitlab
        repository_key = test_repository_key

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

    def it_maps_fetched_pull_request_correctly_to_polaris_pr(self, setup_sync_repos_gitlab):
        _, _ = setup_sync_repos_gitlab
        repository_key = test_repository_key
        gitlab_fetched_pr = {
            "id": 61296045,
            "iid": 69,
            "project_id": 5419303,
            "title": "PO-178 Graphql API updates.",
            "description": "PO-178",
            "state": "merged",
            "created_at": "2020-06-11T18:56:59.410Z",
            "updated_at": "2020-06-11T18:57:08.777Z",
            "merged_by": {
                "id": 1438125,
                "name": "Krishna Kumar",
                "username": "krishnaku",
                "state": "active",
                "avatar_url": "https://secure.gravatar.com/avatar/ff19230d4b6d9a5d7d441dc62fec4619?s=80&d=identicon",
                "web_url": "https://gitlab.com/krishnaku"
            },
            "merged_at": "2020-06-11T18:57:08.818Z",
            "closed_by": "None",
            "closed_at": "None",
            "target_branch": "master",
            "source_branch": "PO-178",
            "user_notes_count": 0,
            "upvotes": 0,
            "downvotes": 0,
            "assignee": "None",
            "author": {
                "id": 1438125,
                "name": "Krishna Kumar",
                "username": "krishnaku",
                "state": "active",
                "avatar_url": "https://secure.gravatar.com/avatar/ff19230d4b6d9a5d7d441dc62fec4619?s=80&d=identicon",
                "web_url": "https://gitlab.com/krishnaku"
            },
            "assignees": [

            ],
            "source_project_id": 5419303,
            "target_project_id": 5419303,
            "labels": [

            ],
            "work_in_progress": False,
            "milestone": "None",
            "merge_when_pipeline_succeeds": False,
            "merge_status": "can_be_merged",
            "sha": "22e505fd9ea218f35bee717d479827101a129af5",
            "merge_commit_sha": "44991b241b824f8cbcb6683edbb55840e9d2a604",
            "squash_commit_sha": "None",
            "discussion_locked": "None",
            "should_remove_source_branch": "None",
            "force_remove_source_branch": False,
            "reference": "!69",
            "references": {
                "short": "!69",
                "relative": "!69",
                "full": "polaris-services/polaris-analytics-service!69"
            },
            "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69",
            "time_stats": {
                "time_estimate": 0,
                "total_time_spent": 0,
                "human_time_estimate": "None",
                "human_total_time_spent": "None"
            },
            "squash": False,
            "task_completion_status": {
                "count": 0,
                "completed_count": 0
            },
            "has_conflicts": False,
            "blocking_discussions_resolved": True,
            "approvals_before_merge": "None"
        }
        expected_mapped_pr = {
            'source_id': 61296045,
            'title': 'PO-178 Graphql API updates.',
            'description': 'PO-178',
            'source_state': 'merged',
            'source_created_at': '2020-06-11T18:56:59.410Z',
            'source_last_updated': '2020-06-11T18:57:08.777Z',
            'source_merge_status': 'can_be_merged',
            'source_merged_at': '2020-06-11T18:57:08.818Z',
            'source_branch': 'PO-178',
            'target_branch': 'master',
            'source_repository_source_id': 5419303,
            'target_repository_source_id': 5419303,
            'web_url': 'https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69'
        }
        repository_provider = repository_factory.get_provider_impl(repository_key)
        mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
        assert mapped_pr == expected_mapped_pr
