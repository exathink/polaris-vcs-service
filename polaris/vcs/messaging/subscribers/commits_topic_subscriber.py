# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging

from polaris.messaging.topics import TopicSubscriber, CommitsTopic
from polaris.messaging.messages import CommitHistoryImported
from polaris.messaging.utils import raise_on_failure
from polaris.vcs.db import api
from polaris.common import db
from polaris.vcs.messaging import publish
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
            return self.process_commit_history_imported(message)

    @staticmethod
    def process_commit_history_imported(message):
        commit_history_imported = message.dict

        organization_key = commit_history_imported.get('organization_key')
        repository_key = commit_history_imported.get('repository_key')
        logger.info(f"Process commits created organization {organization_key} repository {repository_key}")

        with db.orm_session() as session:
            repository = api.find_repository(repository_key, join_this=session)
            if repository and not repository.webhooks_registered:
                publish.sync_pull_request(organization_key, repository_key)