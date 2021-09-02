# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic
from polaris.messaging.messages import CommitsCreated, CommitDetailsCreated, PullRequestsCreated, PullRequestsUpdated
from polaris.messaging.utils import raise_on_failure
from polaris.vcs.db import api

logger = logging.getLogger('polaris.vcs.messaging.analytics_topic_subscriber')

class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_vcs',
            message_classes=[
                CommitsCreated, CommitDetailsCreated,
                PullRequestsCreated, PullRequestsUpdated
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if CommitsCreated.message_type == message.message_type:
            return self.process_commits_created(message)

        elif CommitDetailsCreated.message_type == message.message_type:
            return self.process_commit_details_created(message)

        elif message.message_type in [PullRequestsCreated.message_type, PullRequestsUpdated.message_type]:
            return self.process_pull_request_event(message)

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

    @staticmethod
    def process_pull_request_event(message):
        logger.info(f"Processing Pull Request Ack Event: {message.message_type}")

        return raise_on_failure(
            message,
            api.ack_pull_request_event(message['pull_request_summaries'])
        )
