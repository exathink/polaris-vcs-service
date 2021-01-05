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
from polaris.vcs.messaging.messages import AtlassianConnectRepositoryEvent
from polaris.vcs.messaging.message_listener import VcsTopicSubscriber

mock_consumer = MagicMock(MessageConsumer)
mock_consumer.token_provider = get_token_provider()


# Publish both types of events and validate the changes

class TestAtlassianWebhookEvents:
    class TestBitBucketPullRequestEvents:

        @pytest.yield_fixture()
        def setup(self, setup_sync_repos_bitbucket):
            organization_key, connectors = setup_sync_repos_bitbucket
            connector_key = bitbucket_connector_key
            yield Fixture(
                organization_key=organization_key,
                connector_key=connector_key,
            )

        class TestPullRequestCreated:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup

                payload = {
                    "event": "pullrequest:created",
                    "data": {
                        "pullrequest": {

                            "type": "pullrequest",
                            "description": "",
                            "links": {
                                "decline": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/decline"
                                },
                                "diffstat": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diffstat/krishnaku/polaris-bitbucket-test-1:f1eb826ca587%0Dafd3fcad0f30?from_pullrequest_id=6"
                                },
                                "commits": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/commits"
                                },
                                "self": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6"
                                },
                                "comments": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/comments"
                                },
                                "merge": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/merge"
                                },
                                "html": {
                                    "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/6"
                                },
                                "activity": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/activity"
                                },
                                "request-changes": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/request-changes"
                                },
                                "diff": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diff/krishnaku/polaris-bitbucket-test-1:f1eb826ca587%0Dafd3fcad0f30?from_pullrequest_id=6"
                                },
                                "approve": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/approve"
                                },
                                "statuses": {
                                    "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/statuses"
                                }
                            },
                            "title": "Testing all pullrequest events",
                            "close_source_branch": False,
                            "reviewers": [],
                            "id": 6,
                            "destination": {
                                "commit": {
                                    "hash": "afd3fcad0f30",
                                    "type": "commit",
                                    "links": {
                                        "self": {
                                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/afd3fcad0f30"
                                        },
                                        "html": {
                                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/afd3fcad0f30"
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
                                    "uuid": test_repository_source_id
                                },
                                "branch": {
                                    "name": "master"
                                }
                            },
                            "created_on": "2021-01-04T14:02:22.920738+00:00",
                            "summary": {
                                "raw": "",
                                "markup": "markdown",
                                "html": "",
                                "type": "rendered"
                            },
                            "source": {
                                "commit": {
                                    "hash": "f1eb826ca587",
                                    "type": "commit",
                                    "links": {
                                        "self": {
                                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/f1eb826ca587"
                                        },
                                        "html": {
                                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/f1eb826ca587"
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
                                    "uuid": test_repository_source_id
                                },
                                "branch": {
                                    "name": "test-1"
                                }
                            },
                            "comment_count": 0,
                            "state": "OPEN",
                            "task_count": 0,
                            "participants": [],
                            "reason": "",
                            "updated_on": "2021-01-04T14:02:22.991128+00:00",
                            "merge_commit": None,
                            "closed_by": None
                        },
                        "repository": {
                            "scm": "git",
                            "website": None,
                            "uuid": test_repository_source_id,
                            "project": {
                                "links": {
                                    "self": {
                                        "href": "https://api.bitbucket.org/2.0/workspaces/krishnaku/projects/PROJ"
                                    },
                                    "html": {
                                        "href": "https://bitbucket.org/krishnaku/workspace/projects/PROJ"
                                    },
                                    "avatar": {
                                        "href": "https://bitbucket.org/account/user/krishnaku/projects/PROJ/avatar/32?ts=1596678025"
                                    }
                                },
                                "type": "project",
                                "name": "Polaris test",
                                "key": "PROJ",
                                "uuid": "{de521cd8-419b-4073-815b-26cd92613a71}"
                            },
                            "workspace": {
                                "slug": "krishnaku",
                                "type": "workspace",
                                "name": "KrishnaKum",
                                "links": {
                                    "self": {
                                        "href": "https://api.bitbucket.org/2.0/workspaces/krishnaku"
                                    },
                                    "html": {
                                        "href": "https://bitbucket.org/krishnaku/"
                                    },
                                    "avatar": {
                                        "href": "https://bitbucket.org/workspaces/krishnaku/avatar/?ts=1608842661"
                                    }
                                },
                                "uuid": "{87e0d94b-1037-4a87-8e02-fde842accb37}"
                            },
                            "type": "repository",
                            "is_private": True,
                            "name": "polaris-bitbucket-test-1"
                        },

                    }
                }

                yield Fixture(
                    parent=fixture,
                    new_payload=payload
                )

            def it_creates_new_pr(self, setup):
                fixture = setup
                bitbucket_pull_request_message = fake_send(
                    AtlassianConnectRepositoryEvent(
                        send=dict(
                            atlassian_event_type="pullrequest:created",
                            atlassian_connector_key=fixture.connector_key,
                            atlassian_event=json.dumps(fixture.new_payload)
                        )
                    )
                )
                publisher = mock_publisher()
                channel = mock_channel()

                with patch('polaris.vcs.messaging.publish.publish') as publish:
                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer
                    message = subscriber.dispatch(mock_channel(), bitbucket_pull_request_message)
                    assert message
                    source_pr_id = str(fixture.new_payload['data']['pullrequest']['id'])
                    source_repo_id = str(test_repository_source_id)
                    assert db.connection().execute(
                        f"select count(id) from repos.pull_requests \
                        where source_id='{source_pr_id}' \
                        and source_repository_source_id='{source_repo_id}'").scalar() == 1
                    assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)

            class TestPullRequestFulfilled:

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup

                    payload = {
                        "event": "pullrequest:fulfilled",
                        "data": {
                            "pullrequest": {
                                "rendered": {
                                    "description": {
                                        "raw": "Approve/unapprove can be done without merging too.",
                                        "markup": "markdown",
                                        "html": "<p>Approve/unapprove can be done without merging too.</p>",
                                        "type": "rendered"
                                    },
                                    "title": {
                                        "raw": "Testing all pullrequest events",
                                        "markup": "markdown",
                                        "html": "<p>Testing all pullrequest events</p>",
                                        "type": "rendered"
                                    }
                                },
                                "type": "pullrequest",
                                "description": "Approve/unapprove can be done without merging too.",
                                "links": {
                                    "decline": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/decline"
                                    },
                                    "diffstat": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diffstat/krishnaku/polaris-bitbucket-test-1:a2a21997339a%0Dafd3fcad0f30?from_pullrequest_id=6"
                                    },
                                    "commits": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/commits"
                                    },
                                    "self": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6"
                                    },
                                    "comments": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/comments"
                                    },
                                    "merge": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/merge"
                                    },
                                    "html": {
                                        "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/6"
                                    },
                                    "activity": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/activity"
                                    },
                                    "request-changes": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/request-changes"
                                    },
                                    "diff": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diff/krishnaku/polaris-bitbucket-test-1:a2a21997339a%0Dafd3fcad0f30?from_pullrequest_id=6"
                                    },
                                    "approve": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/approve"
                                    },
                                    "statuses": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/statuses"
                                    }
                                },
                                "title": "Testing all pullrequest events",
                                "close_source_branch": False,
                                "reviewers": [],
                                "id": 6,
                                "destination": {
                                    "commit": {
                                        "hash": "afd3fcad0f30",
                                        "type": "commit",
                                        "links": {
                                            "self": {
                                                "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/afd3fcad0f30"
                                            },
                                            "html": {
                                                "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/afd3fcad0f30"
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
                                        "uuid": test_repository_source_id
                                    },
                                    "branch": {
                                        "name": "master"
                                    }
                                },
                                "created_on": "2021-01-04T14:02:22.920738+00:00",
                                "summary": {
                                    "raw": "Approve/unapprove can be done without merging too.",
                                    "markup": "markdown",
                                    "html": "<p>Approve/unapprove can be done without merging too.</p>",
                                    "type": "rendered"
                                },
                                "source": {
                                    "commit": {
                                        "hash": "f1eb826ca587",
                                        "type": "commit",
                                        "links": {
                                            "self": {
                                                "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/f1eb826ca587"
                                            },
                                            "html": {
                                                "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/f1eb826ca587"
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
                                        "uuid": test_repository_source_id
                                    },
                                    "branch": {
                                        "name": "test-1"
                                    }
                                },
                                "comment_count": 1,
                                "state": "MERGED",
                                "task_count": 0,
                                "participants": [
                                    {
                                        "participated_on": "2021-01-04T14:25:02.003070+00:00",
                                        "state": None,
                                        "role": "PARTICIPANT",
                                        "user": {
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
                                        "type": "participant",
                                        "approved": False
                                    }
                                ],
                                "reason": "",
                                "updated_on": "2021-01-04T14:28:58.136864+00:00",
                                "merge_commit": {
                                    "hash": "a2a21997339a",
                                    "type": "commit",
                                    "links": {
                                        "self": {
                                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/a2a21997339a"
                                        },
                                        "html": {
                                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/a2a21997339a"
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
                                }
                            },
                            "repository": {
                                "scm": "git",
                                "website": None,
                                "uuid": test_repository_source_id,
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
                            }
                        }
                    }
                    yield Fixture(
                        parent=fixture,
                        update_payload=payload
                    )

                def it_updates_status_of_pr(self, setup):
                    fixture = setup

                    bitbucket_new_pull_request_message = fake_send(
                        AtlassianConnectRepositoryEvent(
                            send=dict(
                                atlassian_event_type="pullrequest:created",
                                atlassian_connector_key=fixture.connector_key,
                                atlassian_event=json.dumps(fixture.new_payload)
                            )
                        )
                    )

                    bitbucket_update_pull_request_message = fake_send(
                        AtlassianConnectRepositoryEvent(
                            send=dict(
                                atlassian_event_type="pullrequest:fulfilled",
                                atlassian_connector_key=fixture.connector_key,
                                atlassian_event=json.dumps(fixture.update_payload)
                            )
                        )
                    )

                    publisher = mock_publisher()
                    channel = mock_channel()

                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer

                    # Creating PR
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        subscriber.dispatch(mock_channel(), bitbucket_new_pull_request_message)
                        assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)

                    # Updating PR
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        message = subscriber.dispatch(mock_channel(), bitbucket_update_pull_request_message)
                        assert message
                        source_pr_id = str(fixture.update_payload['data']['pullrequest']['id'])
                        source_repo_id = str(test_repository_source_id)
                        assert db.connection().execute(
                            f"select count(id) from repos.pull_requests \
                                            where source_id='{source_pr_id}' \
                                            and source_repository_source_id='{source_repo_id}' \
                                            and source_state='merged' \
                                            and state='merged'").scalar() == 1
                        assert_topic_and_message(publish, VcsTopic, PullRequestsUpdated)

            class TestPullRequestCommentCreated:
                # Should update last update date

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup

                    payload = {
                        "event": "pullrequest:comment_created",
                        "data": {
                            "comment": {
                                "links": {
                                    "self": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/comments/197155680"
                                    },
                                    "html": {
                                        "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/6/_/diff#comment-197155680"
                                    }
                                },
                                "deleted": False,
                                "pullrequest": {
                                    "type": "pullrequest",
                                    "id": 6,
                                    "links": {
                                        "self": {
                                            "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6"
                                        },
                                        "html": {
                                            "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/6"
                                        }
                                    },
                                    "title": "Testing all pullrequest events"
                                },
                                "content": {
                                    "raw": "test comment updated",
                                    "markup": "markdown",
                                    "html": "<p>test comment updated</p>",
                                    "type": "rendered"
                                },
                                "created_on": "2021-01-04T14:09:15.531201+00:00",
                                "user": {
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
                                "updated_on": "2021-01-04T14:09:15.533606+00:00",
                                "type": "pullrequest_comment",
                                "id": 197155680
                            },
                            "pullrequest": {
                                "type": "pullrequest",
                                "description": "Approve/unapprove can be done without merging too.",
                                "links": {
                                    "decline": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/decline"
                                    },
                                    "diffstat": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diffstat/krishnaku/polaris-bitbucket-test-1:f1eb826ca587%0Dafd3fcad0f30?from_pullrequest_id=6"
                                    },
                                    "commits": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/commits"
                                    },
                                    "self": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6"
                                    },
                                    "comments": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/comments"
                                    },
                                    "merge": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/merge"
                                    },
                                    "html": {
                                        "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/pull-requests/6"
                                    },
                                    "activity": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/activity"
                                    },
                                    "request-changes": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/request-changes"
                                    },
                                    "diff": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/diff/krishnaku/polaris-bitbucket-test-1:f1eb826ca587%0Dafd3fcad0f30?from_pullrequest_id=6"
                                    },
                                    "approve": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/approve"
                                    },
                                    "statuses": {
                                        "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/pullrequests/6/statuses"
                                    }
                                },
                                "title": "Testing all pullrequest events",
                                "close_source_branch": False,
                                "reviewers": [],
                                "id": 6,
                                "destination": {
                                    "commit": {
                                        "hash": "afd3fcad0f30",
                                        "type": "commit",
                                        "links": {
                                            "self": {
                                                "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/afd3fcad0f30"
                                            },
                                            "html": {
                                                "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/afd3fcad0f30"
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
                                        "uuid": test_repository_source_id
                                    },
                                    "branch": {
                                        "name": "master"
                                    }
                                },
                                "created_on": "2021-01-04T14:02:22.920738+00:00",
                                "summary": {
                                    "raw": "Approve/unapprove can be done without merging too.",
                                    "markup": "markdown",
                                    "html": "<p>Approve/unapprove can be done without merging too.</p>",
                                    "type": "rendered"
                                },
                                "source": {
                                    "commit": {
                                        "hash": "f1eb826ca587",
                                        "type": "commit",
                                        "links": {
                                            "self": {
                                                "href": "https://api.bitbucket.org/2.0/repositories/krishnaku/polaris-bitbucket-test-1/commit/f1eb826ca587"
                                            },
                                            "html": {
                                                "href": "https://bitbucket.org/krishnaku/polaris-bitbucket-test-1/commits/f1eb826ca587"
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
                                        "uuid": test_repository_source_id
                                    },
                                    "branch": {
                                        "name": "test-1"
                                    }
                                },
                                "comment_count": 1,
                                "state": "OPEN",
                                "task_count": 0,
                                "participants": [
                                    {
                                        "participated_on": "2021-01-04T14:09:15.533606+00:00",
                                        "state": None,
                                        "role": "PARTICIPANT",
                                        "user": {
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
                                        "type": "participant",
                                        "approved": False
                                    }
                                ],
                                "reason": "",
                                "updated_on": "2021-01-04T14:09:15.533606+00:00",
                                "merge_commit": None,
                                "closed_by": None
                            },
                            "repository": {
                                "scm": "git",
                                "website": None,
                                "uuid": test_repository_source_id,
                                "project": {
                                    "links": {
                                        "self": {
                                            "href": "https://api.bitbucket.org/2.0/workspaces/krishnaku/projects/PROJ"
                                        },
                                        "html": {
                                            "href": "https://bitbucket.org/krishnaku/workspace/projects/PROJ"
                                        },
                                        "avatar": {
                                            "href": "https://bitbucket.org/account/user/krishnaku/projects/PROJ/avatar/32?ts=1596678025"
                                        }
                                    },
                                    "type": "project",
                                    "name": "Polaris test",
                                    "key": "PROJ",
                                    "uuid": "{de521cd8-419b-4073-815b-26cd92613a71}"
                                },
                            },
                        }
                    }
                    yield Fixture(
                        parent=fixture,
                        update_payload=payload
                    )

                def it_updates_status_of_pr(self, setup):
                    fixture = setup

                    bitbucket_new_pull_request_message = fake_send(
                        AtlassianConnectRepositoryEvent(
                            send=dict(
                                atlassian_event_type="pullrequest:created",
                                atlassian_connector_key=fixture.connector_key,
                                atlassian_event=json.dumps(fixture.new_payload)
                            )
                        )
                    )

                    bitbucket_update_pull_request_message = fake_send(
                        AtlassianConnectRepositoryEvent(
                            send=dict(
                                atlassian_event_type="pullrequest:comment_created",
                                atlassian_connector_key=fixture.connector_key,
                                atlassian_event=json.dumps(fixture.update_payload)
                            )
                        )
                    )

                    publisher = mock_publisher()
                    channel = mock_channel()

                    subscriber = VcsTopicSubscriber(mock_channel(), publisher=publisher)
                    subscriber.consumer_context = mock_consumer

                    # Creating PR
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        subscriber.dispatch(mock_channel(), bitbucket_new_pull_request_message)
                        assert_topic_and_message(publish, VcsTopic, PullRequestsCreated)
                    source_pr_id = str(fixture.update_payload['data']['pullrequest']['id'])
                    source_repo_id = str(test_repository_source_id)
                    source_last_updated_date = db.connection().execute(
                        f"select source_last_updated from repos.pull_requests \
                            where source_id='{source_pr_id}' \
                            and source_repository_source_id='{source_repo_id}'"
                    ).fetchall()[0][0]

                    # Updating PR
                    with patch('polaris.vcs.messaging.publish.publish') as publish:
                        message = subscriber.dispatch(mock_channel(), bitbucket_update_pull_request_message)
                        assert message

                        assert db.connection().execute(
                            f"select count(id) from repos.pull_requests \
                                            where source_id='{source_pr_id}' \
                                            and source_repository_source_id='{source_repo_id}' \
                                            and source_last_updated>'{source_last_updated_date}'").scalar() == 1
                        assert_topic_and_message(publish, VcsTopic, PullRequestsUpdated)
