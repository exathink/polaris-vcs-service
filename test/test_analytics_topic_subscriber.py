# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, func
from polaris.messaging.messages import CommitsCreated, CommitDetailsCreated
from polaris.messaging.test_utils import mock_channel, fake_send
from polaris.vcs.messaging.subscribers.analytics_topic_subscriber import AnalyticsTopicSubscriber
from test.shared_fixtures import *

commit_detail_created_common = dict(
    stats=dict(
        deletions=0,
        files=2,
        insertions=21,
        lines=2
    ),
    parents=[],
    source_files=[
        dict(
            key=uuid.uuid4().hex,
            path='test/',
            name='files1.txt',
            file_type='txt',
            version_count=1,
            is_deleted=False,
            action='A',
            stats={"lines": 2, "insertions": 2, "deletions": 0}
        ),
        dict(
            key=uuid.uuid4().hex,
            path='test/',
            name='files2.py',
            file_type='py',
            version_count=1,
            is_deleted=False,
            action='A',
            stats={"lines": 2, "insertions": 2, "deletions": 0}
        )
    ]
)


class TestAnalyticsTopicSubscriber:

    class TestProcessCommitsCreated:

        def it_processes_the_message_correctly(self, setup_commits):
            commits = setup_commits
            payload = dict(
                organization_key=test_organization_key,
                repository_key=test_repository_key,
                branch='master',
                new_commits=commits

            )

            channel = mock_channel()
            message = fake_send(CommitsCreated(send=payload))
            result = AnalyticsTopicSubscriber(channel).dispatch(channel, message)
            assert result['success']
            assert db.connection().execute(
                "select count(id) from repos.commits where analytics_commit_synced_at is NULL "
            ).scalar() == 0

    class TestProcessCommitDetailsCreated:

        def it_processes_the_message_correctly(self, setup_commits):
            commits = setup_commits
            payload = dict(
                organization_key=test_organization_key,
                repository_key=test_repository_key,
                repository_name=test_repository_name,
                commit_details=[
                    dict(key=commit['key'], source_commit_id=commit['source_commit_id'], **commit_detail_created_common)
                    for commit in commits
                ]
            )

            channel = mock_channel()
            message = fake_send(CommitDetailsCreated(send=payload))
            result = AnalyticsTopicSubscriber(channel).dispatch(channel, message)
            assert result['success']
            assert result['updated'] == len(commits)
            assert db.connection().execute(
                "select count(id) from repos.commits where analytics_details_synced_at is NULL "
            ).scalar() == 0




