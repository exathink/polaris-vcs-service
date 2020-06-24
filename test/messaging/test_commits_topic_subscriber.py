# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from ..shared_fixtures import *
from unittest.mock import patch

from polaris.messaging.test_utils import fake_send, mock_channel, mock_publisher
from polaris.vcs.messaging.subscribers import CommitsTopicSubscriber
from polaris.messaging.messages import CommitHistoryImported


class TestCommitHistoryImported:

    def it_fetches_new_pull_requests_for_the_first_run(self, setup_sync_repos_gitlab):
        organization_key, repository_key = setup_sync_repos_gitlab

        message = fake_send(CommitHistoryImported(
            send=dict(
                organization_key=str(test_organization_key),
                repository_key=str(test_repository_key),
                repository_name='Pull requests',
                total_commits=10,
                new_commits=[dict(
                    key='aaaaaaaaa',
                    source_commit_id="xxxx",
                    commit_date=datetime.utcnow(),
                    commit_date_tz_offset=1,
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=1,
                    commit_message='random',
                    created_at=datetime.utcnow(),
                    created_on_branch='pr1',
                    committer_alias_key='xxxx',
                    author_alias_key='xxxxxx'
                )],
                new_contributors=[dict(
                    key='xxxxxxx',
                    name='Test contributor',
                    alias='Test'
                )],
                branch_info=dict(
                    name="PR1",
                    is_new=True,
                    is_default=True,
                    is_stale=False,
                    remote_head='xxxxxxx',
                    is_orphan=False
                )
            )
        ))
        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]

            created_messages, updated_messages = CommitsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 1
            assert len(updated_messages) == 0

    def it_fetches_pull_requests_updated_after_latest_source_last_updated(self, setup_sync_repos_gitlab):
        organization_key, repository_key = setup_sync_repos_gitlab

        message = fake_send(CommitHistoryImported(
            send=dict(
                organization_key=str(test_organization_key),
                repository_key=str(test_repository_key),
                repository_name='Pull requests',
                total_commits=10,
                new_commits=[dict(
                    key='aaaaaaaaa',
                    source_commit_id="xxxx",
                    commit_date=datetime.utcnow(),
                    commit_date_tz_offset=1,
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=1,
                    commit_message='random',
                    created_at=datetime.utcnow(),
                    created_on_branch='pr1',
                    committer_alias_key='xxxx',
                    author_alias_key='xxxxxx'
                )],
                new_contributors=[dict(
                    key='xxxxxxx',
                    name='Test contributor',
                    alias='Test'
                )],
                branch_info=dict(
                    name="PR1",
                    is_new=True,
                    is_default=True,
                    is_stale=False,
                    remote_head='xxxxxxx',
                    is_orphan=False
                )
            )
        ))
        channel = mock_channel()
        publisher = mock_publisher()

        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                [
                    dict(
                        **pull_requests_common_fields
                    )
                ]
            ]

            created_messages, updated_messages = CommitsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 1
            assert len(updated_messages) == 0
        with patch(
                'polaris.vcs.integrations.gitlab.GitlabRepository.fetch_pull_requests_from_source') as fetch_prs:
            fetch_prs.return_value = [
                []
            ]
            created_messages, updated_messages = CommitsTopicSubscriber(channel, publisher).dispatch(channel, message)
            assert len(created_messages) == 0
            assert len(updated_messages) == 0