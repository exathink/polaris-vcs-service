# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging

from polaris.messaging.topics import TopicSubscriber, CommitsTopic
from polaris.messaging.utils import raise_message_processing_error
from polaris.messaging.messages import CommitHistoryImported
from polaris.vcs import commands

logger = logging.getLogger('polaris.vcs.messaging.commits_topic_subscriber')


class CommitsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=CommitsTopic(channel, create=True),
            subscriber_queue='commits_vcs',
            message_classes=[
                CommitHistoryImported
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if CommitHistoryImported.message_type == message.message_type:
            # Calling the nested iterators
            for sync_pull_request_command in self.process_commit_history_imported(message):
                for result in sync_pull_request_command:
                    return result


    @staticmethod
    def process_commit_history_imported(message):
        repository_key = message['repository_key']
        logger.info(f"Processing commit history imported")
        try:
            yield commands.sync_pull_requests(repository_key=repository_key)
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process commit history imported', str(exc))
