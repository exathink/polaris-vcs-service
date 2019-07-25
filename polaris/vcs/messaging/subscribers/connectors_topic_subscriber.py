# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.messaging.topics import TopicSubscriber, ConnectorsTopic, VcsTopic
from polaris.messaging.messages import RepositoryUpdated, RepositoryCreated
from polaris.vcs.messaging.messages import RefreshConnectorRepositories
from polaris.messaging.utils import raise_message_processing_error

from polaris.vcs import commands


logger = logging.getLogger('polaris.vcs.messaging.connectors_topic_subscriber')


class ConnectorsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=ConnectorsTopic(channel, create=True),
            subscriber_queue='connectors_vcs',
            message_classes=[
                RefreshConnectorRepositories
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if RefreshConnectorRepositories.message_type == message.message_type:
            created_messages = []
            updated_messages = []
            for created, updated in self.process_refresh_connector_repositories(message):
                self.publish_responses(created, created_messages, updated, updated_messages)

            return created_messages, updated_messages

    @staticmethod
    def process_refresh_connector_repositories(message):
        connector_key = message['connector_key']
        tracking_receipt_key = message.get('tracking_receipt_key')
        logger.info(
            f"Processing  {message.message_type}: "
            f" Connector Key : {connector_key}"

        )
        try:
            yield from ConnectorsTopicSubscriber.sync_repositories(connector_key,
                                                                   tracking_receipt_key=tracking_receipt_key)
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to sync work items sources', str(exc))

    def publish_responses(self, created, created_messages, updated, updated_messages):
        if len(created) > 0:
            logger.info(f"{len(updated)} repository created")
            for repository in created:
                created_message = RepositoryCreated(
                    send=dict(
                        organization_key=repository['organization_key'],
                        repository=repository
                    )
                )
                self.publish(VcsTopic, created_message)
                created_messages.append(created_message)
        if len(updated) > 0:
            logger.info(f"{len(updated)} repositories updated")
            for repository in updated:
                updated_message = RepositoryUpdated(
                    send=dict(
                        organization_key=repository['organization_key'],
                        repository=repository
                    )
                )
                self.publish(VcsTopic, updated_message)
                updated_messages.append(updated_message)

    @staticmethod
    def sync_repositories(connector_key, tracking_receipt_key=None):
        for repositories in commands.sync_repositories(
                connector_key=connector_key,
                tracking_receipt_key=tracking_receipt_key
        ):
            created = []
            updated = []
            for repository in repositories:
                if repository['is_new']:
                    created.append(repository)
                else:
                    updated.append(repository)

            yield created, updated
