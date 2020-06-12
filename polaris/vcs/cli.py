# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

import argh
from datetime import datetime
from polaris.common import db
from polaris.messaging.topics import CommitsTopic
from polaris.messaging.messages import CommitHistoryImported
from polaris.messaging.utils import publish

from polaris.utils.logging import config_logging

logger = logging.getLogger('polaris.vcs_service.cli')

db.init()


def sync_pull_requests(organization_key=None, repository_key=None):
    publish(
        CommitsTopic,
        CommitHistoryImported(
            send=dict(
                organization_key=str(organization_key),
                repository_key=str(repository_key),
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
        )
    )


if __name__ == '__main__':
    config_logging(
        suppress=['requests.packages.urllib3.connectionpool']
    )

    argh.dispatch_commands([
        sync_pull_requests
    ])
