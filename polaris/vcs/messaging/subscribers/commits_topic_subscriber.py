# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging

from polaris.messaging.topics import TopicSubscriber, CommitsTopic, VcsTopic
from polaris.messaging.utils import raise_message_processing_error
from polaris.messaging.messages import CommitHistoryImported, PullRequestsCreated, PullRequestsUpdated
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
            created_messages = []
            updated_messages = []
            # Calling the nested iterators
            for sync_pull_request_command in self.process_commit_history_imported(message):
                for result in sync_pull_request_command:
                    if result['success']:
                        self.publish_sync_pull_request_responses(message, result['pull_requests'], created_messages, updated_messages)
            return created_messages, updated_messages

    @staticmethod
    def process_commit_history_imported(message):
        repository_key = message['repository_key']
        logger.info(f"Processing commit history imported")
        try:
            yield commands.sync_pull_requests(repository_key=repository_key)
        except Exception as exc:
            raise_message_processing_error(message, 'Failed to process commit history imported', str(exc))

    def publish_sync_pull_request_responses(self, message, synced_pull_requests, created_messages, updated_messages):
        organization_key = message['organization_key']
        repository_key = message['repository_key']
        created = []
        updated = []
        for pr in synced_pull_requests:
            if pr['is_new']:
                created.append(pr)
            else:
                updated.append(pr)
        if len(created) > 0:
            created_message = PullRequestsCreated(
                send=dict(
                    organization_key=organization_key,
                    repository_key=repository_key,
                    new_pull_requests=[
                        dict(
                            key=str(pr['key']),
                            title=pr['title'],
                            description=pr['description'],
                            web_url=pr['web_url'],
                            created_at=pr['source_created_at'],
                            updated_at=pr['source_last_updated'],
                            deleted_at=pr['deleted_at'],
                            state=pr['source_state'],
                            merge_status=pr['source_merge_status'],
                            merged_at=pr['source_merged_at'],
                            source_branch=pr['source_branch'],
                            source_branch_id=pr['source_branch_id'],
                            source_branch_latest_commit=pr['source_branch_latest_commit'],
                            target_branch=pr['target_branch'],
                            target_branch_id=pr['target_branch_id'],
                            source_repository_id=pr['source_repository_id'],
                            source_id=pr['source_id'],
                            display_id=pr['source_display_id'],
                            repository_id=pr['repository_id']
                        )
                        for pr in created
                    ]
                )
            )
            self.publish(VcsTopic, created_message)
            created_messages.append(created_message)
        if len(updated) > 0:
            updated_message = PullRequestsUpdated(
                send=dict(
                    organization_key=organization_key,
                    repository_key=repository_key,
                    updated_pull_requests=[
                        dict(
                            key=str(pr['key']),
                            title=pr['title'],
                            description=pr['description'],
                            web_url=pr['web_url'],
                            created_at=pr['source_created_at'],
                            updated_at=pr['source_last_updated'],
                            deleted_at=pr['deleted_at'],
                            state=pr['source_state'],
                            merge_status=pr['source_merge_status'],
                            merged_at=pr['source_merged_at'],
                            source_branch=pr['source_branch'],
                            source_branch_id=pr['source_branch_id'],
                            source_branch_latest_commit=pr['source_branch_latest_commit'],
                            target_branch=pr['target_branch'],
                            target_branch_id=pr['target_branch_id'],
                            source_repository_id=pr['source_repository_id'],
                            source_id=pr['source_id'],
                            display_id=pr['source_display_id'],
                            repository_id=pr['repository_id']
                        )
                        for pr in updated
                    ]
                )
            )

            self.publish(VcsTopic, updated_message)
            updated_messages.append(updated_message)
