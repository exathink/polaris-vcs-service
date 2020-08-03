# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from unittest.mock import patch

from ..shared_fixtures import *
from polaris.vcs.db import api
from polaris.common.enums import GitlabPullRequestState, GithubPullRequestState


class TestSyncGitlabPullRequests:

    def it_inserts_newly_fetched_pull_requests_into_db(self, setup_org_repo_gitlab):
        repository, organization = setup_org_repo_gitlab
        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "merged",
                "state": GitlabPullRequestState.merged.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": "can_be_merged",
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]

        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert db.connection().execute("select count(*) from repos.pull_requests where source_id='61296045'").scalar() == 1


    def it_updates_pull_request_existing_in_db(self, setup_org_repo_gitlab):
        repository, organization = setup_org_repo_gitlab
        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "open",
                "state": GitlabPullRequestState.open.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": "can_be_merged",
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]


        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert prs[0]['state'] == 'open'
        assert db.connection().execute(
            "select count(*) from repos.pull_requests where source_id='61296045' and source_state='open'").scalar() == 1

        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "merged",
                "state": GitlabPullRequestState.merged.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": "can_be_merged",
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]

        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert not prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert prs[0]['state'] == 'merged'
        assert db.connection().execute(
            "select count(*) from repos.pull_requests where source_id='61296045' and source_state='merged'").scalar() == 1


class TestSyncGithubPullRequests:

    def it_inserts_newly_fetched_pull_requests_into_db(self, setup_org_repo):
        repository, organization = setup_org_repo
        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "merged",
                "state": GithubPullRequestState.merged.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": None,
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://github.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]

        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert db.connection().execute("select count(*) from repos.pull_requests where source_id='61296045'").scalar() == 1


    def it_updates_pull_request_existing_in_db(self, setup_org_repo):
        repository, organization = setup_org_repo
        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "open",
                "state": GithubPullRequestState.open.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": None,
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://github.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]


        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert prs[0]['state'] == 'open'
        assert db.connection().execute(
            "select count(*) from repos.pull_requests where source_id='61296045' and source_state='open'").scalar() == 1

        pull_requests = [
            {
                "source_id": 61296045,
                "source_display_id": "69",
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "source_state": "merged",
                "state": GithubPullRequestState.merged.value,
                "source_created_at": "2020-06-11T18:56:59.410Z",
                "source_last_updated": "2020-06-11T18:57:08.777Z",
                "source_merge_status": None,
                "source_merged_at": "2020-06-11T18:57:08.818Z",
                "source_branch": "PO-178",
                "target_branch": "master",
                "source_repository_source_id": 1000,
                "target_repository_source_id": 1000,
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            },
        ]

        result = api.sync_pull_requests(test_repository_key, iter([pull_requests]))
        assert result['success']
        prs = result['pull_requests']
        assert not prs[0]['is_new']
        assert prs[0]['title'] == pull_requests[0]['title']
        assert prs[0]['state'] == 'merged'
        assert db.connection().execute(
            "select count(*) from repos.pull_requests where source_id='61296045' and source_state='merged'").scalar() == 1
