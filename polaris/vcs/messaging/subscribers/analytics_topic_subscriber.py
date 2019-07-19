# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic


class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_vcs',
            message_classes=[
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        pass
