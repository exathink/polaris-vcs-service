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
from polaris.vcs.messaging.messages import GitlabRepositoryEvent
from polaris.vcs.messaging.message_listener import VcsTopicSubscriber

mock_consumer = MagicMock(MessageConsumer)
mock_consumer.token_provider = get_token_provider()


# Publish both types of events and validate the changes

class TestGitlabWebhookEvents:
    class TestGitlabMergeRequestEvents:

        @pytest.fixture()
        def setup(self, setup_sync_repos_gitlab):
            organization_key, connectors = setup_sync_repos_gitlab
            connector_key = gitlab_connector_key
            event_type = 'merge_request'
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
                    "object_kind": "merge_request",
                    "event_type": "merge_request",
                    "user": {
                        "name": "Pragya Goyal",
                        "username": "pragya3",
                        "avatar_url": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?s=80&d=identicon",
                        "email": "pragya@64sqs.com"
                    },
                    "project": {
                        "id": int(test_repository_source_id),
                        "name": "test-repo",
                        "description": "",
                        "web_url": "https://gitlab.com/polaris-test/test-repo",
                        "avatar_url": None,
                        "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                        "namespace": "polaris-test",
                        "visibility_level": 0,
                        "path_with_namespace": "polaris-test/test-repo",
                        "default_branch": "master",
                        "ci_config_path": None,
                        "homepage": "https://gitlab.com/polaris-test/test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                    },
                    "object_attributes": {
                        "assignee_id": None,
                        "author_id": 5257663,
                        "created_at": "2020-11-24 12:04:38 UTC",
                        "description": "",
                        "head_pipeline_id": None,
                        "id": 79289102,
                        "iid": 15,
                        "last_edited_at": None,
                        "last_edited_by_id": None,
                        "merge_commit_sha": None,
                        "merge_error": None,
                        "merge_params": {
                            "force_remove_source_branch": "0"
                        },
                        "merge_status": "unchecked",
                        "merge_user_id": None,
                        "merge_when_pipeline_succeeds": False,
                        "milestone_id": None,
                        "source_branch": "test-pr-new",
                        "source_project_id": int(test_repository_source_id),
                        "state_id": 1,
                        "target_branch": "master",
                        "target_project_id": int(test_repository_source_id),
                        "time_estimate": 0,
                        "title": "New PR",
                        "updated_at": "2020-11-24 12:04:38 UTC",
                        "updated_by_id": None,
                        "url": "https://gitlab.com/polaris-test/test-repo/-/merge_requests/15",
                        "source": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "target": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "last_commit": {
                            "id": "87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "message": "New PR\n",
                            "title": "New PR",
                            "timestamp": "2020-11-24T17:33:59+05:30",
                            "url": "https://gitlab.com/polaris-test/test-repo/-/commit/87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "author": {
                                "name": "Pragya Goyal",
                                "email": "pragya@64sqs.com"
                            }
                        },
                        "work_in_progress": False,
                        "total_time_spent": 0,
                        "human_total_time_spent": None,
                        "human_time_estimate": None,
                        "assignee_ids": [],
                        "state": "opened",
                        "action": "open"
                    },
                    "labels": [],
                    "changes": {
                        "author_id": {
                            "previous": None,
                            "current": 5257663
                        },
                        "created_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        },
                        "description": {
                            "previous": None,
                            "current": ""
                        },
                        "id": {
                            "previous": None,
                            "current": 79289102
                        },
                        "iid": {
                            "previous": None,
                            "current": 15
                        },
                        "merge_params": {
                            "previous": {},
                            "current": {
                                "force_remove_source_branch": "0"
                            }
                        },
                        "source_branch": {
                            "previous": None,
                            "current": "test-pr-new"
                        },
                        "source_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "target_branch": {
                            "previous": None,
                            "current": "master"
                        },
                        "target_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "title": {
                            "previous": None,
                            "current": "New PR"
                        },
                        "updated_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        }
                    },
                    "repository": {
                        "name": "test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "description": "",
                        "homepage": "https://gitlab.com/polaris-test/test-repo"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_creates_new_pr(self, setup):
                fixture = setup
                gitlab_pull_request_message = fake_send(
                    GitlabRepositoryEvent(
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
                    message = subscriber.dispatch(mock_channel(), gitlab_pull_request_message)
                    assert message
                    source_pr_id = str(fixture.new_payload['object_attributes']['id'])
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
                        "object_kind": "merge_request",
                        "event_type": "merge_request",
                        "user": {
                            "name": "Pragya Goyal",
                            "username": "pragya3",
                            "avatar_url": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?s=80&d=identicon",
                            "email": "pragya@64sqs.com"
                        },
                        "project": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "object_attributes": {
                            "assignee_id": None,
                            "author_id": 5257663,
                            "created_at": "2020-11-24 12:04:38 UTC",
                            "description": "",
                            "head_pipeline_id": None,
                            "id": 79289102,
                            "iid": 15,
                            "last_edited_at": None,
                            "last_edited_by_id": None,
                            "merge_commit_sha": None,
                            "merge_error": None,
                            "merge_params": {
                                "force_remove_source_branch": "0"
                            },
                            "merge_status": "can_be_merged",
                            "merge_user_id": None,
                            "merge_when_pipeline_succeeds": False,
                            "milestone_id": None,
                            "source_branch": "test-pr-new",
                            "source_project_id": int(test_repository_source_id),
                            "state_id": 2,
                            "target_branch": "master",
                            "target_project_id": int(test_repository_source_id),
                            "time_estimate": 0,
                            "title": "New PR",
                            "updated_at": "2020-11-24 13:39:21 UTC",
                            "updated_by_id": None,
                            "url": "https://gitlab.com/polaris-test/test-repo/-/merge_requests/15",
                            "source": {
                                "id": int(test_repository_source_id),
                                "name": "test-repo",
                                "description": "",
                                "web_url": "https://gitlab.com/polaris-test/test-repo",
                                "avatar_url": None,
                                "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                                "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                                "namespace": "polaris-test",
                                "visibility_level": 0,
                                "path_with_namespace": "polaris-test/test-repo",
                                "default_branch": "master",
                                "ci_config_path": None,
                                "homepage": "https://gitlab.com/polaris-test/test-repo",
                                "url": "git@gitlab.com:polaris-test/test-repo.git",
                                "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                                "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                            },
                            "target": {
                                "id": int(test_repository_source_id),
                                "name": "test-repo",
                                "description": "",
                                "web_url": "https://gitlab.com/polaris-test/test-repo",
                                "avatar_url": None,
                                "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                                "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                                "namespace": "polaris-test",
                                "visibility_level": 0,
                                "path_with_namespace": "polaris-test/test-repo",
                                "default_branch": "master",
                                "ci_config_path": None,
                                "homepage": "https://gitlab.com/polaris-test/test-repo",
                                "url": "git@gitlab.com:polaris-test/test-repo.git",
                                "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                                "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                            },
                            "last_commit": {
                                "id": "87d608b790fcf7cb06546567d2a1b1092c5876f8",
                                "message": "New PR\n",
                                "title": "New PR",
                                "timestamp": "2020-11-24T17:33:59+05:30",
                                "url": "https://gitlab.com/polaris-test/test-repo/-/commit/87d608b790fcf7cb06546567d2a1b1092c5876f8",
                                "author": {
                                    "name": "Pragya Goyal",
                                    "email": "pragya@64sqs.com"
                                }
                            },
                            "work_in_progress": False,
                            "total_time_spent": 0,
                            "human_total_time_spent": None,
                            "human_time_estimate": None,
                            "assignee_ids": [],
                            "state": "closed",
                            "action": "close"
                        },
                        "labels": [],
                        "changes": {
                            "state_id": {
                                "previous": 1,
                                "current": 2
                            },
                            "updated_at": {
                                "previous": "2020-11-24 12:04:38 UTC",
                                "current": "2020-11-24 13:39:21 UTC"
                            }
                        },
                        "repository": {
                            "name": "test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "description": "",
                            "homepage": "https://gitlab.com/polaris-test/test-repo"
                        }
                    }

                    yield Fixture(
                        parent=fixture,
                        update_payload=payload
                    )

                def it_updates_existing_pr(self, setup):
                    fixture = setup
                    gitlab_new_pull_request_message = fake_send(
                        GitlabRepositoryEvent(
                            send=dict(
                                event_type=fixture.event_type,
                                connector_key=fixture.connector_key,
                                payload=json.dumps(fixture.new_payload)
                            )
                        )
                    )

                    gitlab_update_pull_request_message = fake_send(
                        GitlabRepositoryEvent(
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
                        subscriber.dispatch(mock_channel(), gitlab_new_pull_request_message)
                        assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)

                    # Updating
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        message = subscriber.dispatch(mock_channel(), gitlab_update_pull_request_message)
                        assert message
                        source_pr_id = str(fixture.update_payload['object_attributes']['id'])
                        source_repo_id = str(test_repository_source_id)
                        assert db.connection().execute(
                            f"select count(id) from repos.pull_requests \
                                                where source_id='{source_pr_id}' \
                                                and source_repository_source_id='{source_repo_id}' \
                                                and state='closed'").scalar() == 1
                        assert_topic_and_message(publish, VcsTopic, PullRequestsUpdated)


    class TestGitlabMergeRequestEventsWhenImportDisabled:
        @pytest.fixture()
        def setup(self, setup_sync_repos_gitlab_disabled):
            organization_key, connectors, repository = setup_sync_repos_gitlab_disabled
            connector_key = gitlab_connector_key
            event_type = 'merge_request'

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
                    "object_kind": "merge_request",
                    "event_type": "merge_request",
                    "user": {
                        "name": "Pragya Goyal",
                        "username": "pragya3",
                        "avatar_url": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?s=80&d=identicon",
                        "email": "pragya@64sqs.com"
                    },
                    "project": {
                        "id": int(test_repository_source_id),
                        "name": "test-repo",
                        "description": "",
                        "web_url": "https://gitlab.com/polaris-test/test-repo",
                        "avatar_url": None,
                        "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                        "namespace": "polaris-test",
                        "visibility_level": 0,
                        "path_with_namespace": "polaris-test/test-repo",
                        "default_branch": "master",
                        "ci_config_path": None,
                        "homepage": "https://gitlab.com/polaris-test/test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                    },
                    "object_attributes": {
                        "assignee_id": None,
                        "author_id": 5257663,
                        "created_at": "2020-11-24 12:04:38 UTC",
                        "description": "",
                        "head_pipeline_id": None,
                        "id": 79289102,
                        "iid": 15,
                        "last_edited_at": None,
                        "last_edited_by_id": None,
                        "merge_commit_sha": None,
                        "merge_error": None,
                        "merge_params": {
                            "force_remove_source_branch": "0"
                        },
                        "merge_status": "unchecked",
                        "merge_user_id": None,
                        "merge_when_pipeline_succeeds": False,
                        "milestone_id": None,
                        "source_branch": "test-pr-new",
                        "source_project_id": int(test_repository_source_id),
                        "state_id": 1,
                        "target_branch": "master",
                        "target_project_id": int(test_repository_source_id),
                        "time_estimate": 0,
                        "title": "New PR",
                        "updated_at": "2020-11-24 12:04:38 UTC",
                        "updated_by_id": None,
                        "url": "https://gitlab.com/polaris-test/test-repo/-/merge_requests/15",
                        "source": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "target": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "last_commit": {
                            "id": "87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "message": "New PR\n",
                            "title": "New PR",
                            "timestamp": "2020-11-24T17:33:59+05:30",
                            "url": "https://gitlab.com/polaris-test/test-repo/-/commit/87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "author": {
                                "name": "Pragya Goyal",
                                "email": "pragya@64sqs.com"
                            }
                        },
                        "work_in_progress": False,
                        "total_time_spent": 0,
                        "human_total_time_spent": None,
                        "human_time_estimate": None,
                        "assignee_ids": [],
                        "state": "opened",
                        "action": "open"
                    },
                    "labels": [],
                    "changes": {
                        "author_id": {
                            "previous": None,
                            "current": 5257663
                        },
                        "created_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        },
                        "description": {
                            "previous": None,
                            "current": ""
                        },
                        "id": {
                            "previous": None,
                            "current": 79289102
                        },
                        "iid": {
                            "previous": None,
                            "current": 15
                        },
                        "merge_params": {
                            "previous": {},
                            "current": {
                                "force_remove_source_branch": "0"
                            }
                        },
                        "source_branch": {
                            "previous": None,
                            "current": "test-pr-new"
                        },
                        "source_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "target_branch": {
                            "previous": None,
                            "current": "master"
                        },
                        "target_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "title": {
                            "previous": None,
                            "current": "New PR"
                        },
                        "updated_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        }
                    },
                    "repository": {
                        "name": "test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "description": "",
                        "homepage": "https://gitlab.com/polaris-test/test-repo"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_ignores_the_event_when_repository_import_is_disabled(self, setup):
                fixture = setup
                gitlab_pull_request_message = fake_send(
                    GitlabRepositoryEvent(
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
                    subscriber = VcsTopicSubscriber(channel, publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(channel, gitlab_pull_request_message)
                    assert not message
                    publisher.assert_not_called()

    class TestGitlabMergeRequestEventsWhenNeverImported:
        @pytest.fixture()
        def setup(self, setup_sync_repos_gitlab_disabled):
            organization_key, connectors, repository = setup_sync_repos_gitlab_disabled
            connector_key = gitlab_connector_key
            event_type = 'merge_request'

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
                    "object_kind": "merge_request",
                    "event_type": "merge_request",
                    "user": {
                        "name": "Pragya Goyal",
                        "username": "pragya3",
                        "avatar_url": "https://secure.gravatar.com/avatar/f4e5904c494e37510101ac9ce50e7ddf?s=80&d=identicon",
                        "email": "pragya@64sqs.com"
                    },
                    "project": {
                        "id": int(test_repository_source_id),
                        "name": "test-repo",
                        "description": "",
                        "web_url": "https://gitlab.com/polaris-test/test-repo",
                        "avatar_url": None,
                        "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                        "namespace": "polaris-test",
                        "visibility_level": 0,
                        "path_with_namespace": "polaris-test/test-repo",
                        "default_branch": "master",
                        "ci_config_path": None,
                        "homepage": "https://gitlab.com/polaris-test/test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                        "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                    },
                    "object_attributes": {
                        "assignee_id": None,
                        "author_id": 5257663,
                        "created_at": "2020-11-24 12:04:38 UTC",
                        "description": "",
                        "head_pipeline_id": None,
                        "id": 79289102,
                        "iid": 15,
                        "last_edited_at": None,
                        "last_edited_by_id": None,
                        "merge_commit_sha": None,
                        "merge_error": None,
                        "merge_params": {
                            "force_remove_source_branch": "0"
                        },
                        "merge_status": "unchecked",
                        "merge_user_id": None,
                        "merge_when_pipeline_succeeds": False,
                        "milestone_id": None,
                        "source_branch": "test-pr-new",
                        "source_project_id": int(test_repository_source_id),
                        "state_id": 1,
                        "target_branch": "master",
                        "target_project_id": int(test_repository_source_id),
                        "time_estimate": 0,
                        "title": "New PR",
                        "updated_at": "2020-11-24 12:04:38 UTC",
                        "updated_by_id": None,
                        "url": "https://gitlab.com/polaris-test/test-repo/-/merge_requests/15",
                        "source": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "target": {
                            "id": int(test_repository_source_id),
                            "name": "test-repo",
                            "description": "",
                            "web_url": "https://gitlab.com/polaris-test/test-repo",
                            "avatar_url": None,
                            "git_ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "git_http_url": "https://gitlab.com/polaris-test/test-repo.git",
                            "namespace": "polaris-test",
                            "visibility_level": 0,
                            "path_with_namespace": "polaris-test/test-repo",
                            "default_branch": "master",
                            "ci_config_path": None,
                            "homepage": "https://gitlab.com/polaris-test/test-repo",
                            "url": "git@gitlab.com:polaris-test/test-repo.git",
                            "ssh_url": "git@gitlab.com:polaris-test/test-repo.git",
                            "http_url": "https://gitlab.com/polaris-test/test-repo.git"
                        },
                        "last_commit": {
                            "id": "87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "message": "New PR\n",
                            "title": "New PR",
                            "timestamp": "2020-11-24T17:33:59+05:30",
                            "url": "https://gitlab.com/polaris-test/test-repo/-/commit/87d608b790fcf7cb06546567d2a1b1092c5876f8",
                            "author": {
                                "name": "Pragya Goyal",
                                "email": "pragya@64sqs.com"
                            }
                        },
                        "work_in_progress": False,
                        "total_time_spent": 0,
                        "human_total_time_spent": None,
                        "human_time_estimate": None,
                        "assignee_ids": [],
                        "state": "opened",
                        "action": "open"
                    },
                    "labels": [],
                    "changes": {
                        "author_id": {
                            "previous": None,
                            "current": 5257663
                        },
                        "created_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        },
                        "description": {
                            "previous": None,
                            "current": ""
                        },
                        "id": {
                            "previous": None,
                            "current": 79289102
                        },
                        "iid": {
                            "previous": None,
                            "current": 15
                        },
                        "merge_params": {
                            "previous": {},
                            "current": {
                                "force_remove_source_branch": "0"
                            }
                        },
                        "source_branch": {
                            "previous": None,
                            "current": "test-pr-new"
                        },
                        "source_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "target_branch": {
                            "previous": None,
                            "current": "master"
                        },
                        "target_project_id": {
                            "previous": None,
                            "current": int(test_repository_source_id)
                        },
                        "title": {
                            "previous": None,
                            "current": "New PR"
                        },
                        "updated_at": {
                            "previous": None,
                            "current": "2020-11-24 12:04:38 UTC"
                        }
                    },
                    "repository": {
                        "name": "test-repo",
                        "url": "git@gitlab.com:polaris-test/test-repo.git",
                        "description": "",
                        "homepage": "https://gitlab.com/polaris-test/test-repo"
                    }
                }
                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_ignores_the_event_when_repository_has_never_been_imported(self, setup):
                fixture = setup
                gitlab_pull_request_message = fake_send(
                    GitlabRepositoryEvent(
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
                    subscriber = VcsTopicSubscriber(channel, publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(channel, gitlab_pull_request_message)
                    assert not message
                    publisher.assert_not_called()
