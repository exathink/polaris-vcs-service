# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from unittest.mock import patch

from polaris.vcs import commands, repository_factory
from ..shared_fixtures import *


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
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert prs[0]['is_new']
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
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert not prs[0]['is_new']

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
            "closed_by": None,
            "closed_at": None,
            "target_branch": "master",
            "source_branch": "PO-178",
            "user_notes_count": 0,
            "upvotes": 0,
            "downvotes": 0,
            "assignee": None,
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
            "milestone": None,
            "merge_when_pipeline_succeeds": False,
            "merge_status": "can_be_merged",
            "sha": "22e505fd9ea218f35bee717d479827101a129af5",
            "merge_commit_sha": "44991b241b824f8cbcb6683edbb55840e9d2a604",
            "squash_commit_sha": None,
            "discussion_locked": None,
            "should_remove_source_branch": None,
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
                "human_time_estimate": None,
                "human_total_time_spent": None
            },
            "squash": False,
            "task_completion_status": {
                "count": 0,
                "completed_count": 0
            },
            "has_conflicts": False,
            "blocking_discussions_resolved": True,
            "approvals_before_merge": None
        }
        expected_mapped_pr = {
            'source_id': 61296045,
            "source_display_id": 69,
            'title': 'PO-178 Graphql API updates.',
            'description': 'PO-178',
            'source_state': 'merged',
            'state': 'merged',
            'source_created_at': '2020-06-11T18:56:59.410Z',
            'source_last_updated': '2020-06-11T18:57:08.777Z',
            'source_merge_status': 'can_be_merged',
            'source_merged_at': '2020-06-11T18:57:08.818Z',
            'source_closed_at':None,
            'end_date': '2020-06-11T18:57:08.818Z',
            'source_branch': 'PO-178',
            'target_branch': 'master',
            'source_repository_source_id': 5419303,
            'target_repository_source_id': 5419303,
            'web_url': 'https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69'
           }
        repository_provider = repository_factory.get_provider_impl(repository_key)
        mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
        assert mapped_pr == expected_mapped_pr

    class TestGitlabPRDateHandling:

        def it_maps_end_dates_and_state_when_pr_is_open(self, setup_sync_repos_gitlab):
            _, _ = setup_sync_repos_gitlab
            repository_key = test_repository_key
            gitlab_fetched_pr = {
                "id": 61296045,
                "iid": 69,
                "project_id": 5419303,
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "state": "opened",
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
                "merged_at": None,
                "closed_by": None,
                "closed_at": None,
                "target_branch": "master",
                "source_branch": "PO-178",
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            }
            repository_provider = repository_factory.get_provider_impl(repository_key)
            mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
            assert mapped_pr['end_date'] is None
            assert mapped_pr['state'] == 'open'
            assert mapped_pr['source_merged_at'] is None
            assert mapped_pr['source_closed_at'] is None
            assert mapped_pr['source_last_updated'] is not None

        def it_maps_end_dates_and_state_when_pr_is_merged_and_merged_at_is_provided(self, setup_sync_repos_gitlab):
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
                "merged_at": "2020-06-11T18:57:08.777Z",
                "closed_by": None,
                "closed_at": None,
                "target_branch": "master",
                "source_branch": "PO-178",
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            }
            repository_provider = repository_factory.get_provider_impl(repository_key)
            mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
            assert mapped_pr['end_date'] is not None
            assert mapped_pr['state'] == 'merged'
            assert mapped_pr['source_merged_at'] is not None
            assert mapped_pr['source_closed_at'] is None
            assert mapped_pr['source_last_updated'] is not None

        def it_maps_end_dates_and_state_when_pr_is_merged_and_merged_at_is__not_provided(self, setup_sync_repos_gitlab):
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
                "merged_at": None,
                "closed_by": None,
                "closed_at": None,
                "target_branch": "master",
                "source_branch": "PO-178",
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            }
            repository_provider = repository_factory.get_provider_impl(repository_key)
            mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
            assert mapped_pr['end_date'] is not None
            assert mapped_pr['state'] == 'merged'
            assert mapped_pr['source_merged_at'] is not None
            assert mapped_pr['source_closed_at'] is None
            assert mapped_pr['source_last_updated'] is not None

        def it_maps_end_dates_and_state_when_pr_is_closed_and_closed_at_is_provided(self, setup_sync_repos_gitlab):
            _, _ = setup_sync_repos_gitlab
            repository_key = test_repository_key
            gitlab_fetched_pr = {
                "id": 61296045,
                "iid": 69,
                "project_id": 5419303,
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "state": "closed",
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
                "merged_at": None,
                "closed_by": None,
                "closed_at": "2020-06-11T18:57:08.777Z",
                "target_branch": "master",
                "source_branch": "PO-178",
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            }
            repository_provider = repository_factory.get_provider_impl(repository_key)
            mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
            assert mapped_pr['end_date'] is not None
            assert mapped_pr['state'] == 'closed'
            assert mapped_pr['source_merged_at'] is None
            assert mapped_pr['source_closed_at'] is not None
            assert mapped_pr['source_last_updated'] is not None

        def it_maps_end_dates_and_state_when_pr_is_closed_and_closed_at_is__not_provided(self, setup_sync_repos_gitlab):
            _, _ = setup_sync_repos_gitlab
            repository_key = test_repository_key
            gitlab_fetched_pr = {
                "id": 61296045,
                "iid": 69,
                "project_id": 5419303,
                "title": "PO-178 Graphql API updates.",
                "description": "PO-178",
                "state": "closed",
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
                "merged_at": None,
                "closed_by": None,
                "closed_at": None,
                "target_branch": "master",
                "source_branch": "PO-178",
                "web_url": "https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            }
            repository_provider = repository_factory.get_provider_impl(repository_key)
            mapped_pr = repository_provider.map_pull_request_info(gitlab_fetched_pr)
            assert mapped_pr['end_date'] is not None
            assert mapped_pr['state'] == 'closed'
            assert mapped_pr['source_merged_at'] is None
            assert mapped_pr['source_closed_at'] is not None
            assert mapped_pr['source_last_updated'] is not None


class DictToObj(object):
    def __init__(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
               setattr(self, a, [DictToObj(x) if isinstance(x, dict) else x for x in b])
            else:
               setattr(self, a, DictToObj(b) if isinstance(b, dict) else b)


class TestSyncGithubPullRequests:

    def it_fetches_latest_updated_pull_requests_from_github(self, setup_sync_repos):
        _, _ = setup_sync_repos
        repository_key = test_repository_key

        with patch(
                'polaris.vcs.integrations.github.GithubRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert prs[0]['is_new']
        updated_pull_request = dict(
            **pull_requests_common_fields
        )
        updated_pull_request['source_last_updated'] = datetime.utcnow()
        with patch(
                'polaris.vcs.integrations.github.GithubRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **updated_pull_request
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert not prs[0]['is_new']

    def it_maps_fetched_pull_request_correctly_to_polaris_pr(self, setup_sync_repos):
        _, _ = setup_sync_repos
        repository_key = test_repository_key
        github_fetched_pr_dict = dict(
            id=457807963,
            number=5,
            title='Create pull_requests.txt',
            body='',
            state='open',
            created_at=datetime(2020, 7, 28, 13, 26, 8),
            updated_at=datetime(2020, 7, 28, 13, 26, 8),
            merged_at=None,
            closed_at=None,
            head=dict(ref='pr_test', repo=dict(id=195584868)),
            base=dict(ref='master', repo=dict(id=195584868)),
            html_url="https://github.com/exathink/urjuna-test1/pulls/5"
        )
        github_fetched_pr = DictToObj(github_fetched_pr_dict)
        expected_mapped_pr = {
            "source_id": 457807963,
            "source_display_id": 5,
            "title": "Create pull_requests.txt",
            "description": "",
            "source_state": "open",
            "state": "open",
            "source_created_at": "2020-07-28 13:26:08",
            "source_last_updated": "2020-07-28 13:26:08",
            "source_merge_status": None,
            "source_merged_at": None,
            "source_closed_at": None,
            "end_date": None,
            "source_branch": "pr_test",
            "target_branch": "master",
            "source_repository_source_id": 195584868,
            "target_repository_source_id": 195584868,
            "web_url": "https://github.com/exathink/urjuna-test1/pulls/5"
        }
        repository_provider = repository_factory.get_provider_impl(repository_key)
        mapped_pr = repository_provider.map_pull_request_info(github_fetched_pr)
        assert mapped_pr == expected_mapped_pr


class TestSyncBitBucketPullRequests:

    def it_fetches_latest_updated_pull_requests_from_bitbucket(self, setup_sync_repos_bitbucket):
        _, _ = setup_sync_repos_bitbucket
        repository_key = test_repository_key

        with patch(
                'polaris.vcs.integrations.atlassian.BitBucketRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert prs[0]['is_new']
        updated_pull_request = dict(
            **pull_requests_common_fields
        )
        updated_pull_request['source_last_updated'] = datetime.utcnow()
        with patch(
                'polaris.vcs.integrations.atlassian.BitBucketRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **updated_pull_request
                    )
                ]
            ]
            for command_output in commands.sync_pull_requests(repository_key):
                result = command_output
                assert result['success']
                prs = result['pull_requests']
                assert not prs[0]['is_new']

    def it_maps_fetched_pull_request_correctly_to_polaris_pr(self, setup_sync_repos_bitbucket):
        _, _ = setup_sync_repos_bitbucket
        repository_key = test_repository_key
        bitbucket_fetched_pr = {
            "description": "",
            "links": {
                "decline": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/decline"
                },
                "diffstat": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diffstat/krishnaku/polaris-bitbucket-test-1:de434b82b25c%0D6933bbf37775?from_pullrequest_id=1"
                },
                "commits": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/commits"
                },
                "self": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1"
                },
                "comments": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/comments"
                },
                "merge": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/merge"
                },
                "html": {
                    "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/1"
                },
                "activity": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/activity"
                },
                "diff": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diff/krishnaku/polaris-bitbucket-test-1:de434b82b25c%0D6933bbf37775?from_pullrequest_id=1"
                },
                "approve": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/approve"
                },
                "statuses": {
                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/1/statuses"
                }
            },
            "title": "Test commit 1",
            "close_source_branch": False,
            "type": "pullrequest",
            "id": 1,
            "destination": {
                "commit": {
                    "hash": "6933bbf37775",
                    "type": "commit",
                    "links": {
                        "self": {
                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/6933bbf37775"
                        },
                        "html": {
                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/6933bbf37775"
                        }
                    }
                },
                "repository": {
                    "links": {
                        "self": {
                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1"
                        },
                        "html": {
                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1"
                        },
                        "avatar": {
                            "href": "https://bytebucket.org/ravatar/%7B9b9b3553-735b-486a-83fa-f5a404c48a72%7D?ts=default"
                        }
                    },
                    "type": "repository",
                    "name": "polaris-bitbucket-test-1",
                    "full_name": "krishnaku/polaris-bitbucket-test-1",
                    "uuid": "{9b9b3553-735b-486a-83fa-f5a404c48a72}"
                },
                "branch": {
                    "name": "master"
                }
            },
            "created_on": "2020-08-10T09:35:34.475346+00:00",
            "summary": {
                "raw": "",
                "markup": "markdown",
                "html": "",
                "type": "rendered"
            },
            "source": {
                "commit": {
                    "hash": "09eba088adce",
                    "type": "commit",
                    "links": {
                        "self": {
                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/09eba088adce"
                        },
                        "html": {
                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/09eba088adce"
                        }
                    }
                },
                "repository": {
                    "links": {
                        "self": {
                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1"
                        },
                        "html": {
                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1"
                        },
                        "avatar": {
                            "href": "https://bytebucket.org/ravatar/%7B9b9b3553-735b-486a-83fa-f5a404c48a72%7D?ts=default"
                        }
                    },
                    "type": "repository",
                    "name": "polaris-bitbucket-test-1",
                    "full_name": "krishnaku/polaris-bitbucket-test-1",
                    "uuid": "{9b9b3553-735b-486a-83fa-f5a404c48a72}"
                },
                "branch": {
                    "name": "test-1"
                }
            },
            "comment_count": 0,
            "state": "MERGED",
            "task_count": 0,
            "reason": "",
            "updated_on": "2020-08-10T11:57:08.400283+00:00",
            "author": {
                "display_name": "Pragya Goyal",
                "uuid": "{1d53c158-5b57-4613-a45f-1c1b69b34ec1}",
                "links": {
                    "self": {
                        "href": "https://api.bitbucket.org/2.0/users/%7B1d53c158-5b57-4613-a45f-1c1b69b34ec1%7D"
                    },
                    "html": {
                        "href": "https://bitbucket.org/%7B1d53c158-5b57-4613-a45f-1c1b69b34ec1%7D/"
                    },
                    "avatar": {
                        "href": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?d=https%3A%2F%2Favatar-management--avatars.us-west-2.prod.public.atl-paas.net%2Finitials%2FPG-2.png"
                    }
                },
                "nickname": "pragya",
                "type": "user",
                "account_id": "5e176e8885a8c90ecaca3c63"
            },
            "merge_commit": {
                "hash": "de434b82b25c",
                "type": "commit",
                "links": {
                    "self": {
                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/de434b82b25c"
                    },
                    "html": {
                        "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/de434b82b25c"
                    }
                }
            },
            "closed_by": {
                "display_name": "Pragya Goyal",
                "uuid": "{1d53c158-5b57-4613-a45f-1c1b69b34ec1}",
                "links": {
                    "self": {
                        "href": "https://api.bitbucket.org/2.0/users/%7B1d53c158-5b57-4613-a45f-1c1b69b34ec1%7D"
                    },
                    "html": {
                        "href": "https://bitbucket.org/%7B1d53c158-5b57-4613-a45f-1c1b69b34ec1%7D/"
                    },
                    "avatar": {
                        "href": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?d=https%3A%2F%2Favatar-management--avatars.us-west-2.prod.public.atl-paas.net%2Finitials%2FPG-2.png"
                    }
                },
                "nickname": "pragya",
                "type": "user",
                "account_id": "5e176e8885a8c90ecaca3c63"
            },
            "closed_at": None
        }

        expected_mapped_pr = {
            'source_id': 1,
            'source_display_id': 1,
            'title': 'Test commit 1',
            'description': '',
            'source_state': 'merged',
            'state': 'merged',
            'source_created_at': '2020-08-10T09:35:34.475346+00:00',
            'source_last_updated': '2020-08-10T11:57:08.400283+00:00',
            'source_merge_status': 'can_be_merged',
            'source_merged_at': '2020-08-10T11:57:08.400283+00:00',
            'source_closed_at': None,
            'end_date': '2020-08-10T11:57:08.400283+00:00',
            'source_branch': 'test-1',
            'target_branch': 'master',
            'source_repository_source_id': '{9b9b3553-735b-486a-83fa-f5a404c48a72}',
            'target_repository_source_id': '{9b9b3553-735b-486a-83fa-f5a404c48a72}',
            'web_url': 'https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/1'
        }
        repository_provider = repository_factory.get_provider_impl(repository_key)
        mapped_pr = repository_provider.map_pull_request_info(bitbucket_fetched_pr)
        assert mapped_pr == expected_mapped_pr


class TestSyncPullRequestOtherCases:

    # This is a legal test because there are repos in production for which there are no
    # connectors. These were legacy repos imported before there was a connector architecture in place
    # PRs cannot be pulled from these repos. But we still need to handle them so that we dont get
    # unnecessary error noises in production.

    def it_returns_an_empty_list_when_there_is_no_connector_for_the_repo(self, setup_org_repo_no_connector):
        _, _ = setup_org_repo_no_connector
        repository_key = test_repository_key

        for command_output in commands.sync_pull_requests(repository_key):
            result = command_output
            assert len(result) == 0

