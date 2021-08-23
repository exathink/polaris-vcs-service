# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from ..shared_fixtures import *
from unittest.mock import patch

from polaris.messaging.test_utils import fake_send, mock_channel, mock_publisher
from polaris.vcs.messaging.subscribers import VcsTopicSubscriber
from polaris.messaging.messages import PullRequestsCreated
from polaris.vcs.messaging.messages import SyncPullRequests
from polaris.messaging.topics import VcsTopic


class TestSyncPullRequests:

    def it_fetches_new_pull_requests_for_the_first_run(self, setup_sync_repos_gitlab):
        organization_key, repository_key = setup_sync_repos_gitlab

        message = fake_send(SyncPullRequests(
            send=dict(
                organization_key=str(test_organization_key),
                repository_key=str(test_repository_key),
            )
        ))
        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]

            created_messages, updated_messages = VcsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 1
            assert len(updated_messages) == 0
            publisher.assert_topic_called_with_message(VcsTopic, PullRequestsCreated)

    def it_fetches_pull_requests_updated_after_latest_source_last_updated(self, setup_sync_repos_gitlab):

        message = fake_send(SyncPullRequests(
            send=dict(
                organization_key=str(test_organization_key),
                repository_key=str(test_repository_key),
            )
        ))
        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]

            created_messages, updated_messages = VcsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 1
            assert len(updated_messages) == 0
            publisher.assert_topic_called_with_message(VcsTopic, PullRequestsCreated)
        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                []
            ]
            created_messages, updated_messages = VcsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 0
            assert len(updated_messages) == 0