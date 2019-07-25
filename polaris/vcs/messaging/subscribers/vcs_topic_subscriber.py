# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.messaging.topics import TopicSubscriber, VcsTopic
from polaris.vcs.messaging.messages import RefreshConnectorRepositories

logger = logging.getLogger('polaris.vcs.messaging.vcs_topic_subscriber')


class VcsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=VcsTopic(channel, create=True),
            subscriber_queue='vcs_vcs',
            message_classes=[
                RefreshConnectorRepositories,
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        pass
