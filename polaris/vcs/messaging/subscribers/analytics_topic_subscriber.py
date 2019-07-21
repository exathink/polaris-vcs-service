# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic
from polaris.messaging.messages import CommitsCreated, CommitDetailsCreated
from polaris.messaging.utils import raise_on_failure
from polaris.vcs.db import api


class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_vcs',
            message_classes=[
                CommitsCreated, CommitDetailsCreated
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if CommitsCreated.message_type == message.message_type:
            return self.process_commits_created(message)

        elif CommitDetailsCreated.message_type == message.message_type:
            return self.process_commit_details_created(message)

    @staticmethod
    def process_commits_created(message):
        commits_created = message.dict

        return raise_on_failure(
            message,
            api.ack_commits_created([
                commit['key'] for commit in commits_created['new_commits']
            ])
        )

    @staticmethod
    def process_commit_details_created(message):
        commit_details_created = message.dict

        return raise_on_failure(
            message,
            api.ack_commits_details_created([
                commit['key'] for commit in commit_details_created['commit_details']
            ])
        )
