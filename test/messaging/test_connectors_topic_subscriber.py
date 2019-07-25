# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.shared_fixtures import *
from unittest.mock import patch
from polaris.utils.collections import dict_merge

from polaris.messaging.test_utils import fake_send, mock_channel, mock_publisher
from polaris.vcs.messaging.messages import RefreshConnectorRepositories
from polaris.vcs.messaging.subscribers import ConnectorsTopicSubscriber
from polaris.messaging.topics import VcsTopic
from polaris.messaging.messages import RepositoryCreated, RepositoryUpdated


class TestRefreshConnectorRepositories:

    def it_fetches_new_repos_and_publishes_repos_created_message(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos

        message = fake_send(RefreshConnectorRepositories(
            send=dict(
                connector_key=connectors['github']
            )
        ))

        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.github.GithubRepositoriesConnector.fetch_repositories_from_source') as fetch_repos:
            fetch_repos.return_value = [
                [
                    dict(
                        **repositories_common_fields
                    )
                ]
            ]

            created, updated = ConnectorsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created) == 1
            assert len(updated) == 0
            publisher.assert_topic_called_with_message(VcsTopic, RepositoryCreated)

    def it_updates_existing_repos_and_publishes_repos_updated_message(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos

        message = fake_send(RefreshConnectorRepositories(
            send=dict(
                connector_key=connectors['github']
            )
        ))

        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.github.GithubRepositoriesConnector.fetch_repositories_from_source') as fetch_repos:
            fetch_repos.return_value = [
                [

                    dict_merge(
                        repositories_common_fields,
                        dict(
                            source_id=test_repository_source_id
                        )
                    )
                ]
            ]

            created, updated = ConnectorsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created) == 0
            assert len(updated) == 1
            publisher.assert_topic_called_with_message(VcsTopic, RepositoryUpdated)

    def it_works_for_a_mix_new_and_existing_repos(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos

        message = fake_send(RefreshConnectorRepositories(
            send=dict(
                connector_key=connectors['github']
            )
        ))

        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.github.GithubRepositoriesConnector.fetch_repositories_from_source') as fetch_repos:
            fetch_repos.return_value = [
                [
                    dict(
                        **repositories_common_fields
                    ),
                    dict_merge(
                        repositories_common_fields,
                        dict(
                            source_id=test_repository_source_id
                        )
                    )
                ]
            ]

            created, updated = ConnectorsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created) == 1
            assert len(updated) == 1
            publisher.assert_topic_called_with_message(VcsTopic, RepositoryCreated, call=0)
            publisher.assert_topic_called_with_message(VcsTopic, RepositoryUpdated, call=1)