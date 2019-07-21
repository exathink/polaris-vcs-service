# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.repos.db.schema import commits
from sqlalchemy import update
from datetime import datetime


def ack_commits_created(session, commit_keys):
    updated = session.connection.execute(
        update(commits).values(
            dict(
                analytics_commit_synced_at=datetime.utcnow()
            )
        ).where(
            commits.c.key.in_(
                commit_keys
            )
        )
    ).rowcount
    return {
        'success': True,
        'updated': updated,
        'commit_keys': commit_keys
    }


def ack_commit_details_created(session, commit_keys):
    updated = session.connection.execute(
        update(commits).values(
            dict(
                analytics_details_synced_at=datetime.utcnow()
            )
        ).where(
            commits.c.key.in_(
                commit_keys
            )
        )
    ).rowcount
    return {
        'success': True,
        'updated': updated,
        'commit_keys': commit_keys
    }



