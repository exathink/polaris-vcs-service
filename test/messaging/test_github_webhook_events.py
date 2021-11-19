# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import json
import pytest

from unittest.mock import MagicMock, patch
from ..shared_fixtures import *
from polaris.utils.collections import Fixture

from polaris.messaging.message_consumer import MessageConsumer
from polaris.utils.token_provider import get_token_provider
from polaris.messaging.test_utils import fake_send, mock_publisher, mock_channel, assert_topic_and_message

from polaris.messaging.topics import VcsTopic
from polaris.messaging.messages import PullRequestsCreated, PullRequestsUpdated
from polaris.vcs.messaging.messages import GithubRepositoryEvent, RemoteRepositoryPushEvent
from polaris.vcs.messaging.message_listener import VcsTopicSubscriber

mock_consumer = MagicMock(MessageConsumer)
mock_consumer.token_provider = get_token_provider()


# Publish both types of events and validate the changes

class TestGithubWebhookEvents:
    class TestGithubPullRequestEvents:

        @pytest.fixture()
        def setup(self, setup_sync_repos):
            organization_key, connectors = setup_sync_repos
            connector_key = github_connector_key
            event_type = 'pull_request'
            yield Fixture(
                organization_key=organization_key,
                connector_key=connector_key,
                event_type=event_type
            )

        class TestNewPullRequestEvent:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup

                payload = {
                    "action": "opened",
                    "number": 9,
                    "pull_request": {
                        "url": "https://api.github.com/repos/exathink/test-repo/pulls/9",
                        "id": 535119091,
                        "node_id": "MDExOlB1bGxSZXF1ZXN0NTM1MTE5MDkx",
                        "html_url": "https://github.com/exathink/test-repo/pull/9",
                        "diff_url": "https://github.com/exathink/test-repo/pull/9.diff",
                        "patch_url": "https://github.com/exathink/test-repo/pull/9.patch",
                        "issue_url": "https://api.github.com/repos/exathink/test-repo/issues/9",
                        "number": 9,
                        "state": "open",
                        "locked": False,
                        "title": "Revert \"Revert \"Create file1.txt\"\"",
                        "body": "Reverts exathink/test-repo#8",
                        "created_at": "2020-12-09T11:42:24Z",
                        "updated_at": "2020-12-09T11:42:24Z",
                        "closed_at": None,
                        "merged_at": None,
                        "merge_commit_sha": None,
                        "assignee": None,
                        "assignees": [

                        ],
                        "requested_reviewers": [

                        ],
                        "requested_teams": [

                        ],
                        "labels": [

                        ],
                        "milestone": None,
                        "draft": False,
                        "commits_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits",
                        "review_comments_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments",
                        "review_comment_url": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}",
                        "comments_url": "https://api.github.com/repos/exathink/test-repo/issues/9/comments",
                        "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                        "head": {
                            "label": "exathink:revert-8-revert-7-pragyag-patch-1",
                            "ref": "revert-8-revert-7-pragyag-patch-1",
                            "sha": "6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "base": {
                            "label": "exathink:master",
                            "ref": "master",
                            "sha": "ecde98f84a5a31937d4b47d99b098a0b103d7809",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "_links": {
                            "self": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9"
                            },
                            "html": {
                                "href": "https://github.com/exathink/test-repo/pull/9"
                            },
                            "issue": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9"
                            },
                            "comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9/comments"
                            },
                            "review_comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments"
                            },
                            "review_comment": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}"
                            },
                            "commits": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits"
                            },
                            "statuses": {
                                "href": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7"
                            }
                        },
                        "author_association": "CONTRIBUTOR",
                        "active_lock_reason": None,
                        "merged": False,
                        "mergeable": None,
                        "rebaseable": None,
                        "mergeable_state": "unknown",
                        "merged_by": None,
                        "comments": 0,
                        "review_comments": 0,
                        "maintainer_can_modify": False,
                        "commits": 1,
                        "additions": 1,
                        "deletions": 0,
                        "changed_files": 1
                    },
                    "repository": {
                        "id": int(test_repository_source_id),
                        "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                        "name": "test-repo",
                        "full_name": "exathink/test-repo",
                        "private": True,
                        "default_branch": "master"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_creates_new_pr(self, setup):
                fixture = setup
                github_pull_request_message = fake_send(
                    GithubRepositoryEvent(
                        send=dict(
                            event_type=fixture.event_type,
                            connector_key=fixture.connector_key,
                            payload=json.dumps(fixture.new_payload)
                        )
                    )
                )
                publisher = mock_publisher()
                channel = mock_channel()

                with patch('polaris.vcs.messaging.publish.publish') as publish:
                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(mock_channel(), github_pull_request_message)
                    assert message
                    source_pr_id = str(fixture.new_payload['pull_request']['id'])
                    source_repo_id = str(test_repository_source_id)
                    assert db.connection().execute(
                        f"select count(id) from repos.pull_requests \
                        where source_id='{source_pr_id}' \
                        and source_repository_source_id='{source_repo_id}'").scalar() == 1
                    assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)

            class TestUpdatePullRequestEvent:
                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup

                    payload = {
                        "action": "closed",
                        "number": 9,
                        "pull_request": {
                            "url": "https://api.github.com/repos/exathink/test-repo/pulls/9",
                            "id": 535119091,
                            "node_id": "MDExOlB1bGxSZXF1ZXN0NTM1MTE5MDkx",
                            "html_url": "https://github.com/exathink/test-repo/pull/9",
                            "diff_url": "https://github.com/exathink/test-repo/pull/9.diff",
                            "patch_url": "https://github.com/exathink/test-repo/pull/9.patch",
                            "issue_url": "https://api.github.com/repos/exathink/test-repo/issues/9",
                            "number": 9,
                            "state": "closed",
                            "locked": False,
                            "title": "Revert \"Revert \"Create file1.txt\"\"",
                            "body": "Reverts exathink/test-repo#8",
                            "created_at": "2020-12-09T11:42:24Z",
                            "updated_at": "2020-12-09T12:46:55Z",
                            "closed_at": "2020-12-09T12:46:55Z",
                            "merged_at": "2020-12-09T12:46:55Z",
                            "merge_commit_sha": "8b58dd0d3de97da727a80c8f845c34d9c7b83b69",
                            "assignee": None,
                            "assignees": [

                            ],
                            "requested_reviewers": [

                            ],
                            "requested_teams": [

                            ],
                            "labels": [

                            ],
                            "milestone": None,
                            "draft": False,
                            "commits_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits",
                            "review_comments_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments",
                            "review_comment_url": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}",
                            "comments_url": "https://api.github.com/repos/exathink/test-repo/issues/9/comments",
                            "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                            "head": {
                                "label": "exathink:revert-8-revert-7-pragyag-patch-1",
                                "ref": "revert-8-revert-7-pragyag-patch-1",
                                "sha": "6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                                "user": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "repo": {
                                    "id": int(test_repository_source_id),
                                    "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                    "name": "test-repo",
                                    "full_name": "exathink/test-repo",
                                    "private": True,
                                    "owner": {
                                        "login": "exathink",
                                        "id": 34690089,
                                        "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                        "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                        "gravatar_id": "",
                                        "url": "https://api.github.com/users/exathink",
                                        "html_url": "https://github.com/exathink",
                                        "followers_url": "https://api.github.com/users/exathink/followers",
                                        "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                        "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                        "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                        "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                        "organizations_url": "https://api.github.com/users/exathink/orgs",
                                        "repos_url": "https://api.github.com/users/exathink/repos",
                                        "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                        "received_events_url": "https://api.github.com/users/exathink/received_events",
                                        "type": "Organization",
                                        "site_admin": False
                                    },
                                    "html_url": "https://github.com/exathink/test-repo",
                                    "description": "For testing Github integrations",
                                    "fork": False,
                                    "url": "https://api.github.com/repos/exathink/test-repo",
                                    "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                    "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                    "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                    "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                    "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                    "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                    "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                    "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                    "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                    "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                    "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                    "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                    "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                    "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                    "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                    "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                    "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                    "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                    "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                    "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                    "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                    "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                    "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                    "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                    "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                    "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                    "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                    "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                    "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                    "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                    "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                    "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                    "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                    "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                    "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                    "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                    "created_at": "2020-07-28T11:02:17Z",
                                    "updated_at": "2020-12-09T11:26:22Z",
                                    "pushed_at": "2020-12-09T12:46:56Z",
                                    "git_url": "git://github.com/exathink/test-repo.git",
                                    "ssh_url": "git@github.com:exathink/test-repo.git",
                                    "clone_url": "https://github.com/exathink/test-repo.git",
                                    "svn_url": "https://github.com/exathink/test-repo",
                                    "homepage": None,
                                    "size": 9,
                                    "stargazers_count": 0,
                                    "watchers_count": 0,
                                    "language": None,
                                    "has_issues": True,
                                    "has_projects": True,
                                    "has_downloads": True,
                                    "has_wiki": True,
                                    "has_pages": False,
                                    "forks_count": 0,
                                    "mirror_url": None,
                                    "archived": False,
                                    "disabled": False,
                                    "open_issues_count": 1,
                                    "license": None,
                                    "forks": 0,
                                    "open_issues": 1,
                                    "watchers": 0,
                                    "default_branch": "master",
                                    "allow_squash_merge": True,
                                    "allow_merge_commit": True,
                                    "allow_rebase_merge": True,
                                    "delete_branch_on_merge": False
                                }
                            },
                            "base": {
                                "label": "exathink:master",
                                "ref": "master",
                                "sha": "ecde98f84a5a31937d4b47d99b098a0b103d7809",
                                "user": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "repo": {
                                    "id": int(test_repository_source_id),
                                    "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                    "name": "test-repo",
                                    "full_name": "exathink/test-repo",
                                    "private": True,
                                    "owner": {
                                        "login": "exathink",
                                        "id": 34690089,
                                        "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                        "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                        "gravatar_id": "",
                                        "url": "https://api.github.com/users/exathink",
                                        "html_url": "https://github.com/exathink",
                                        "followers_url": "https://api.github.com/users/exathink/followers",
                                        "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                        "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                        "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                        "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                        "organizations_url": "https://api.github.com/users/exathink/orgs",
                                        "repos_url": "https://api.github.com/users/exathink/repos",
                                        "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                        "received_events_url": "https://api.github.com/users/exathink/received_events",
                                        "type": "Organization",
                                        "site_admin": False
                                    },
                                    "html_url": "https://github.com/exathink/test-repo",
                                    "description": "For testing Github integrations",
                                    "fork": False,
                                    "url": "https://api.github.com/repos/exathink/test-repo",
                                    "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                    "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                    "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                    "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                    "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                    "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                    "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                    "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                    "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                    "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                    "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                    "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                    "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                    "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                    "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                    "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                    "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                    "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                    "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                    "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                    "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                    "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                    "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                    "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                    "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                    "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                    "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                    "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                    "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                    "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                    "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                    "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                    "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                    "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                    "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                    "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                    "created_at": "2020-07-28T11:02:17Z",
                                    "updated_at": "2020-12-09T11:26:22Z",
                                    "pushed_at": "2020-12-09T12:46:56Z",
                                    "git_url": "git://github.com/exathink/test-repo.git",
                                    "ssh_url": "git@github.com:exathink/test-repo.git",
                                    "clone_url": "https://github.com/exathink/test-repo.git",
                                    "svn_url": "https://github.com/exathink/test-repo",
                                    "homepage": None,
                                    "size": 9,
                                    "stargazers_count": 0,
                                    "watchers_count": 0,
                                    "language": None,
                                    "has_issues": True,
                                    "has_projects": True,
                                    "has_downloads": True,
                                    "has_wiki": True,
                                    "has_pages": False,
                                    "forks_count": 0,
                                    "mirror_url": None,
                                    "archived": False,
                                    "disabled": False,
                                    "open_issues_count": 1,
                                    "license": None,
                                    "forks": 0,
                                    "open_issues": 1,
                                    "watchers": 0,
                                    "default_branch": "master",
                                    "allow_squash_merge": True,
                                    "allow_merge_commit": True,
                                    "allow_rebase_merge": True,
                                    "delete_branch_on_merge": False
                                }
                            },
                            "_links": {
                                "self": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/pulls/9"
                                },
                                "html": {
                                    "href": "https://github.com/exathink/test-repo/pull/9"
                                },
                                "issue": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/issues/9"
                                },
                                "comments": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/issues/9/comments"
                                },
                                "review_comments": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments"
                                },
                                "review_comment": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}"
                                },
                                "commits": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits"
                                },
                                "statuses": {
                                    "href": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7"
                                }
                            },
                            "author_association": "CONTRIBUTOR",
                            "active_lock_reason": None,
                            "merged": True,
                            "mergeable": None,
                            "rebaseable": None,
                            "mergeable_state": "unknown",
                            "merged_by": {
                                "login": "pragyag",
                                "id": 2979863,
                                "node_id": "MDQ6VXNlcjI5Nzk4NjM=",
                                "avatar_url": "https://avatars0.githubusercontent.com/u/2979863?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/pragyag",
                                "html_url": "https://github.com/pragyag",
                                "followers_url": "https://api.github.com/users/pragyag/followers",
                                "following_url": "https://api.github.com/users/pragyag/following{/other_user}",
                                "gists_url": "https://api.github.com/users/pragyag/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/pragyag/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/pragyag/subscriptions",
                                "organizations_url": "https://api.github.com/users/pragyag/orgs",
                                "repos_url": "https://api.github.com/users/pragyag/repos",
                                "events_url": "https://api.github.com/users/pragyag/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/pragyag/received_events",
                                "type": "User",
                                "site_admin": False
                            },
                            "comments": 0,
                            "review_comments": 0,
                            "maintainer_can_modify": False,
                            "commits": 1,
                            "additions": 1,
                            "deletions": 0,
                            "changed_files": 1
                        },
                        "repository": {
                            "id": int(test_repository_source_id),
                            "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                            "name": "test-repo",
                            "full_name": "exathink/test-repo",
                            "private": True,
                            "default_branch": "master"
                        }
                    }

                    yield Fixture(
                        parent=fixture,
                        update_payload=payload
                    )

                def it_updates_existing_pr(self, setup):
                    fixture = setup
                    github_new_pull_request_message = fake_send(
                        GithubRepositoryEvent(
                            send=dict(
                                event_type=fixture.event_type,
                                connector_key=fixture.connector_key,
                                payload=json.dumps(fixture.new_payload)
                            )
                        )
                    )

                    github_update_pull_request_message = fake_send(
                        GithubRepositoryEvent(
                            send=dict(
                                event_type=fixture.event_type,
                                connector_key=fixture.connector_key,
                                payload=json.dumps(fixture.update_payload)
                            )
                        )
                    )
                    publisher = mock_publisher()
                    channel = mock_channel()

                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer

                    # Creating PR
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        subscriber.dispatch(mock_channel(), github_new_pull_request_message)
                        assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)

                    # Updating
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        message = subscriber.dispatch(mock_channel(), github_update_pull_request_message)
                        assert message
                        source_pr_id = str(fixture.update_payload['pull_request']['id'])
                        source_repo_id = str(test_repository_source_id)
                        assert db.connection().execute(
                            f"select count(id) from repos.pull_requests \
                                                where source_id='{source_pr_id}' \
                                                and source_repository_source_id='{source_repo_id}' \
                                                and state='merged'").scalar() == 1
                        assert_topic_and_message(publish, VcsTopic, PullRequestsUpdated)

    class TestGithubRepoPushEvent:

        @pytest.fixture()
        def setup(self, setup_sync_repos):
            organization_key, connectors = setup_sync_repos
            connector_key = github_connector_key
            event_type = 'push'
            payload = {
                "ref": "refs/heads/master",
                "before": "126ef38c5dd27f637de1d6f8bdd573737d2bf20f",
                "after": "2e85ab0334abe37977f7c51b09cc4b8f8cae0764",
                "repository": {
                    "id": int(test_repository_source_id)
                }
            }
            yield Fixture(
                organization_key=organization_key,
                connector_key=connector_key,
                event_type=event_type,
                payload=payload
            )

        def it_publishes_remote_repository_push_event(self, setup):
            fixture = setup
            github_repo_push_message = fake_send(
                GithubRepositoryEvent(
                    send=dict(
                        event_type=fixture.event_type,
                        connector_key=fixture.connector_key,
                        payload=json.dumps(fixture.payload)
                    )
                )
            )
            publisher = mock_publisher()

            with patch('polaris.vcs.messaging.publish.publish') as publish:
                subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                subscriber.consumer_context = mock_consumer
                subscriber.dispatch(mock_channel(), github_repo_push_message)
                assert_topic_and_message(publish, VcsTopic, RemoteRepositoryPushEvent)

    class TestGithubPullRequestEventsWhenImportDisabled:

        @pytest.fixture()
        def setup(self, setup_sync_repos_disabled):
            organization_key, connectors, repository = setup_sync_repos_disabled

            connector_key = github_connector_key
            event_type = 'pull_request'

            with db.orm_session() as session:
                session.add(repository)
                repository.import_state = RepositoryImportState.IMPORT_DISABLED

            yield Fixture(
                organization_key=organization_key,
                connector_key=connector_key,
                event_type=event_type
            )

        class TestNewPullRequestEvent:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup

                payload = {
                    "action": "opened",
                    "number": 9,
                    "pull_request": {
                        "url": "https://api.github.com/repos/exathink/test-repo/pulls/9",
                        "id": 535119091,
                        "node_id": "MDExOlB1bGxSZXF1ZXN0NTM1MTE5MDkx",
                        "html_url": "https://github.com/exathink/test-repo/pull/9",
                        "diff_url": "https://github.com/exathink/test-repo/pull/9.diff",
                        "patch_url": "https://github.com/exathink/test-repo/pull/9.patch",
                        "issue_url": "https://api.github.com/repos/exathink/test-repo/issues/9",
                        "number": 9,
                        "state": "open",
                        "locked": False,
                        "title": "Revert \"Revert \"Create file1.txt\"\"",
                        "body": "Reverts exathink/test-repo#8",
                        "created_at": "2020-12-09T11:42:24Z",
                        "updated_at": "2020-12-09T11:42:24Z",
                        "closed_at": None,
                        "merged_at": None,
                        "merge_commit_sha": None,
                        "assignee": None,
                        "assignees": [

                        ],
                        "requested_reviewers": [

                        ],
                        "requested_teams": [

                        ],
                        "labels": [

                        ],
                        "milestone": None,
                        "draft": False,
                        "commits_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits",
                        "review_comments_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments",
                        "review_comment_url": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}",
                        "comments_url": "https://api.github.com/repos/exathink/test-repo/issues/9/comments",
                        "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                        "head": {
                            "label": "exathink:revert-8-revert-7-pragyag-patch-1",
                            "ref": "revert-8-revert-7-pragyag-patch-1",
                            "sha": "6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "base": {
                            "label": "exathink:master",
                            "ref": "master",
                            "sha": "ecde98f84a5a31937d4b47d99b098a0b103d7809",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "_links": {
                            "self": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9"
                            },
                            "html": {
                                "href": "https://github.com/exathink/test-repo/pull/9"
                            },
                            "issue": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9"
                            },
                            "comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9/comments"
                            },
                            "review_comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments"
                            },
                            "review_comment": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}"
                            },
                            "commits": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits"
                            },
                            "statuses": {
                                "href": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7"
                            }
                        },
                        "author_association": "CONTRIBUTOR",
                        "active_lock_reason": None,
                        "merged": False,
                        "mergeable": None,
                        "rebaseable": None,
                        "mergeable_state": "unknown",
                        "merged_by": None,
                        "comments": 0,
                        "review_comments": 0,
                        "maintainer_can_modify": False,
                        "commits": 1,
                        "additions": 1,
                        "deletions": 0,
                        "changed_files": 1
                    },
                    "repository": {
                        "id": int(test_repository_source_id),
                        "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                        "name": "test-repo",
                        "full_name": "exathink/test-repo",
                        "private": True,
                        "default_branch": "master"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_ignores_event_when_import_is_disabled(self, setup):
                fixture = setup
                github_pull_request_message = fake_send(
                    GithubRepositoryEvent(
                        send=dict(
                            event_type=fixture.event_type,
                            connector_key=fixture.connector_key,
                            payload=json.dumps(fixture.new_payload)
                        )
                    )
                )
                publisher = mock_publisher()
                channel = mock_channel()

                with patch('polaris.vcs.messaging.publish.publish') as publish:
                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(mock_channel(), github_pull_request_message)
                    assert not message
                    publisher.assert_not_called()


    class TestGithubPullRequestEventsWhenNeverImported:

        @pytest.fixture()
        def setup(self, setup_sync_repos_disabled):
            organization_key, connectors, repository = setup_sync_repos_disabled

            connector_key = github_connector_key
            event_type = 'pull_request'

            with db.orm_session() as session:
                session.add(repository)
                repository.last_imported = None

            yield Fixture(
                organization_key=organization_key,
                connector_key=connector_key,
                event_type=event_type
            )

        class TestNewPullRequestEvent:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup

                payload = {
                    "action": "opened",
                    "number": 9,
                    "pull_request": {
                        "url": "https://api.github.com/repos/exathink/test-repo/pulls/9",
                        "id": 535119091,
                        "node_id": "MDExOlB1bGxSZXF1ZXN0NTM1MTE5MDkx",
                        "html_url": "https://github.com/exathink/test-repo/pull/9",
                        "diff_url": "https://github.com/exathink/test-repo/pull/9.diff",
                        "patch_url": "https://github.com/exathink/test-repo/pull/9.patch",
                        "issue_url": "https://api.github.com/repos/exathink/test-repo/issues/9",
                        "number": 9,
                        "state": "open",
                        "locked": False,
                        "title": "Revert \"Revert \"Create file1.txt\"\"",
                        "body": "Reverts exathink/test-repo#8",
                        "created_at": "2020-12-09T11:42:24Z",
                        "updated_at": "2020-12-09T11:42:24Z",
                        "closed_at": None,
                        "merged_at": None,
                        "merge_commit_sha": None,
                        "assignee": None,
                        "assignees": [

                        ],
                        "requested_reviewers": [

                        ],
                        "requested_teams": [

                        ],
                        "labels": [

                        ],
                        "milestone": None,
                        "draft": False,
                        "commits_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits",
                        "review_comments_url": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments",
                        "review_comment_url": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}",
                        "comments_url": "https://api.github.com/repos/exathink/test-repo/issues/9/comments",
                        "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                        "head": {
                            "label": "exathink:revert-8-revert-7-pragyag-patch-1",
                            "ref": "revert-8-revert-7-pragyag-patch-1",
                            "sha": "6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "base": {
                            "label": "exathink:master",
                            "ref": "master",
                            "sha": "ecde98f84a5a31937d4b47d99b098a0b103d7809",
                            "user": {
                                "login": "exathink",
                                "id": 34690089,
                                "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                "gravatar_id": "",
                                "url": "https://api.github.com/users/exathink",
                                "html_url": "https://github.com/exathink",
                                "followers_url": "https://api.github.com/users/exathink/followers",
                                "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                "organizations_url": "https://api.github.com/users/exathink/orgs",
                                "repos_url": "https://api.github.com/users/exathink/repos",
                                "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                "received_events_url": "https://api.github.com/users/exathink/received_events",
                                "type": "Organization",
                                "site_admin": False
                            },
                            "repo": {
                                "id": int(test_repository_source_id),
                                "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                                "name": "test-repo",
                                "full_name": "exathink/test-repo",
                                "private": True,
                                "owner": {
                                    "login": "exathink",
                                    "id": 34690089,
                                    "node_id": "MDEyOk9yZ2FuaXphdGlvbjM0NjkwMDg5",
                                    "avatar_url": "https://avatars2.githubusercontent.com/u/34690089?v=4",
                                    "gravatar_id": "",
                                    "url": "https://api.github.com/users/exathink",
                                    "html_url": "https://github.com/exathink",
                                    "followers_url": "https://api.github.com/users/exathink/followers",
                                    "following_url": "https://api.github.com/users/exathink/following{/other_user}",
                                    "gists_url": "https://api.github.com/users/exathink/gists{/gist_id}",
                                    "starred_url": "https://api.github.com/users/exathink/starred{/owner}{/repo}",
                                    "subscriptions_url": "https://api.github.com/users/exathink/subscriptions",
                                    "organizations_url": "https://api.github.com/users/exathink/orgs",
                                    "repos_url": "https://api.github.com/users/exathink/repos",
                                    "events_url": "https://api.github.com/users/exathink/events{/privacy}",
                                    "received_events_url": "https://api.github.com/users/exathink/received_events",
                                    "type": "Organization",
                                    "site_admin": False
                                },
                                "html_url": "https://github.com/exathink/test-repo",
                                "description": "For testing Github integrations",
                                "fork": False,
                                "url": "https://api.github.com/repos/exathink/test-repo",
                                "forks_url": "https://api.github.com/repos/exathink/test-repo/forks",
                                "keys_url": "https://api.github.com/repos/exathink/test-repo/keys{/key_id}",
                                "collaborators_url": "https://api.github.com/repos/exathink/test-repo/collaborators{/collaborator}",
                                "teams_url": "https://api.github.com/repos/exathink/test-repo/teams",
                                "hooks_url": "https://api.github.com/repos/exathink/test-repo/hooks",
                                "issue_events_url": "https://api.github.com/repos/exathink/test-repo/issues/events{/number}",
                                "events_url": "https://api.github.com/repos/exathink/test-repo/events",
                                "assignees_url": "https://api.github.com/repos/exathink/test-repo/assignees{/user}",
                                "branches_url": "https://api.github.com/repos/exathink/test-repo/branches{/branch}",
                                "tags_url": "https://api.github.com/repos/exathink/test-repo/tags",
                                "blobs_url": "https://api.github.com/repos/exathink/test-repo/git/blobs{/sha}",
                                "git_tags_url": "https://api.github.com/repos/exathink/test-repo/git/tags{/sha}",
                                "git_refs_url": "https://api.github.com/repos/exathink/test-repo/git/refs{/sha}",
                                "trees_url": "https://api.github.com/repos/exathink/test-repo/git/trees{/sha}",
                                "statuses_url": "https://api.github.com/repos/exathink/test-repo/statuses/{sha}",
                                "languages_url": "https://api.github.com/repos/exathink/test-repo/languages",
                                "stargazers_url": "https://api.github.com/repos/exathink/test-repo/stargazers",
                                "contributors_url": "https://api.github.com/repos/exathink/test-repo/contributors",
                                "subscribers_url": "https://api.github.com/repos/exathink/test-repo/subscribers",
                                "subscription_url": "https://api.github.com/repos/exathink/test-repo/subscription",
                                "commits_url": "https://api.github.com/repos/exathink/test-repo/commits{/sha}",
                                "git_commits_url": "https://api.github.com/repos/exathink/test-repo/git/commits{/sha}",
                                "comments_url": "https://api.github.com/repos/exathink/test-repo/comments{/number}",
                                "issue_comment_url": "https://api.github.com/repos/exathink/test-repo/issues/comments{/number}",
                                "contents_url": "https://api.github.com/repos/exathink/test-repo/contents/{+path}",
                                "compare_url": "https://api.github.com/repos/exathink/test-repo/compare/{base}...{head}",
                                "merges_url": "https://api.github.com/repos/exathink/test-repo/merges",
                                "archive_url": "https://api.github.com/repos/exathink/test-repo/{archive_format}{/ref}",
                                "downloads_url": "https://api.github.com/repos/exathink/test-repo/downloads",
                                "issues_url": "https://api.github.com/repos/exathink/test-repo/issues{/number}",
                                "pulls_url": "https://api.github.com/repos/exathink/test-repo/pulls{/number}",
                                "milestones_url": "https://api.github.com/repos/exathink/test-repo/milestones{/number}",
                                "notifications_url": "https://api.github.com/repos/exathink/test-repo/notifications{?since,all,participating}",
                                "labels_url": "https://api.github.com/repos/exathink/test-repo/labels{/name}",
                                "releases_url": "https://api.github.com/repos/exathink/test-repo/releases{/id}",
                                "deployments_url": "https://api.github.com/repos/exathink/test-repo/deployments",
                                "created_at": "2020-07-28T11:02:17Z",
                                "updated_at": "2020-12-09T11:26:22Z",
                                "pushed_at": "2020-12-09T11:31:00Z",
                                "git_url": "git://github.com/exathink/test-repo.git",
                                "ssh_url": "git@github.com:exathink/test-repo.git",
                                "clone_url": "https://github.com/exathink/test-repo.git",
                                "svn_url": "https://github.com/exathink/test-repo",
                                "homepage": None,
                                "size": 9,
                                "stargazers_count": 0,
                                "watchers_count": 0,
                                "language": None,
                                "has_issues": True,
                                "has_projects": True,
                                "has_downloads": True,
                                "has_wiki": True,
                                "has_pages": False,
                                "forks_count": 0,
                                "mirror_url": None,
                                "archived": False,
                                "disabled": False,
                                "open_issues_count": 2,
                                "license": None,
                                "forks": 0,
                                "open_issues": 2,
                                "watchers": 0,
                                "default_branch": "master",
                                "allow_squash_merge": True,
                                "allow_merge_commit": True,
                                "allow_rebase_merge": True,
                                "delete_branch_on_merge": False
                            }
                        },
                        "_links": {
                            "self": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9"
                            },
                            "html": {
                                "href": "https://github.com/exathink/test-repo/pull/9"
                            },
                            "issue": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9"
                            },
                            "comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/issues/9/comments"
                            },
                            "review_comments": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/comments"
                            },
                            "review_comment": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/comments{/number}"
                            },
                            "commits": {
                                "href": "https://api.github.com/repos/exathink/test-repo/pulls/9/commits"
                            },
                            "statuses": {
                                "href": "https://api.github.com/repos/exathink/test-repo/statuses/6e6f6a50b6cb451b7eec8cfdd5a57841035f1ff7"
                            }
                        },
                        "author_association": "CONTRIBUTOR",
                        "active_lock_reason": None,
                        "merged": False,
                        "mergeable": None,
                        "rebaseable": None,
                        "mergeable_state": "unknown",
                        "merged_by": None,
                        "comments": 0,
                        "review_comments": 0,
                        "maintainer_can_modify": False,
                        "commits": 1,
                        "additions": 1,
                        "deletions": 0,
                        "changed_files": 1
                    },
                    "repository": {
                        "id": int(test_repository_source_id),
                        "node_id": "MDEwOlJlcG9zaXRvcnkyODMxODY1Nzk=",
                        "name": "test-repo",
                        "full_name": "exathink/test-repo",
                        "private": True,
                        "default_branch": "master"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_ignores_event_when_repository_has_never_been_imported(self, setup):
                fixture = setup
                github_pull_request_message = fake_send(
                    GithubRepositoryEvent(
                        send=dict(
                            event_type=fixture.event_type,
                            connector_key=fixture.connector_key,
                            payload=json.dumps(fixture.new_payload)
                        )
                    )
                )
                publisher = mock_publisher()
                channel = mock_channel()

                with patch('polaris.vcs.messaging.publish.publish') as publish:
                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(mock_channel(), github_pull_request_message)
                    assert not message
                    publisher.assert_not_called()
