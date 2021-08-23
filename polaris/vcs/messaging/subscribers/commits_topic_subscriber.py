# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging

from polaris.messaging.topics import TopicSubscriber, CommitsTopic

logger = logging.getLogger('polaris.vcs.messaging.commits_topic_subscriber')


class CommitsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=CommitsTopic(channel, create=True),
            subscriber_queue='commits_vcs',
            message_classes=[

            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        pass


