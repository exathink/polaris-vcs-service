# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json
import logging

from polaris.messaging.topics import TopicSubscriber, VcsTopic
from polaris.vcs.messaging.messages import AtlassianConnectRepositoryEvent, GitlabRepositoryEvent, \
    RemoteRepositoryPushEvent, GithubRepositoryEvent
from polaris.messaging.utils import raise_message_processing_error
from polaris.vcs import commands
from polaris.vcs.integrations.atlassian import bitbucket_message_handler
from polaris.vcs.integrations.gitlab import gitlab_message_handler
from polaris.vcs.integrations.github import github_message_handler

logger = logging.getLogger('polaris.vcs.messaging.vcs_topic_subscriber')


class VcsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=VcsTopic(channel, create=True),
            subscriber_queue='vcs_vcs',
            message_classes=[
                AtlassianConnectRepositoryEvent,
                GitlabRepositoryEvent,
                GithubRepositoryEvent,
                RemoteRepositoryPushEvent
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if AtlassianConnectRepositoryEvent.message_type == message.message_type:
            return self.process_atlassian_connect_repository_event(message)
        elif GitlabRepositoryEvent.message_type == message.message_type:
            return self.process_gitlab_repository_event(message)
        elif GithubRepositoryEvent.message_type == message.message_type:
            return self.process_github_repository_event(message)
        elif RemoteRepositoryPushEvent.message_type == message.message_type:
            return self.process_remote_repository_push_event(message)

    @staticmethod
    def process_atlassian_connect_repository_event(message):
        connector_key = message['atlassian_connector_key']
        event_type = message['atlassian_event_type']
        event = message['atlassian_event']

        logger.info(
            f"Processing  {message.message_type}: "
            f" Connector Key : {connector_key}"
        )
        try:
            return bitbucket_message_handler.handle_atlassian_connect_repository_event(
                connector_key,
                event_type,
                event
            )
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process repository event', str(exc))

    @staticmethod
    def process_gitlab_repository_event(message):
        connector_key = message['connector_key']
        event_type = message['event_type']
        payload = message['payload']

        logger.info(
            f"Processing  gitlab event {message.message_type}: "
        )
        try:
            return gitlab_message_handler.handle_gitlab_event(
                connector_key,
                event_type,
                payload
            )
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process gitlab repository event', str(exc))

    @staticmethod
    def process_github_repository_event(message):
        connector_key = message['connector_key']
        event_type = message['event_type']
        payload = message['payload']

        logger.info(
            f"Processing  github event {message.message_type}: "
        )
        try:
            return github_message_handler.handle_github_event(
                connector_key,
                event_type,
                payload
            )
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process gitlab repository event', str(exc))

    @staticmethod
    def process_remote_repository_push_event(message):
        connector_key = message['connector_key']
        repository_source_id = message['repository_source_id']

        logger.info(
            f"Processing  repository push event for connector {connector_key} "
        )
        try:
            return commands.handle_remote_repository_push(connector_key, repository_source_id)
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process repository push event', str(exc))
