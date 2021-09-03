# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
import logging
from datetime import datetime, timedelta
from polaris.repos.db.model import Repository, PullRequest, pull_requests, repositories
from polaris.common import db
from sqlalchemy import select, and_, or_, Column, Integer, cast, func, Interval, bindparam
from sqlalchemy.dialects.postgresql import insert

from polaris.utils.exceptions import ProcessingException

log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def find_pull_request(session, pull_request_key):
    return PullRequest.find_by_pull_request_key(session, pull_request_key)


"""
    This is a common function to extract a pull_request_summary from a database row. This is the inverse
    operation of the summary object that is passed to the database operations from a pull request message
    and is used to return an dict that can be passed as a payload for a pull request message.
"""


def pull_request_summary(pull_request_row, is_new, repository_key, key=None):
    return dict(
        is_new=is_new,
        key=key or pull_request_row.key,
        title=pull_request_row.title,
        description=pull_request_row.description,
        web_url=pull_request_row.web_url,
        created_at=pull_request_row.source_created_at,
        updated_at=pull_request_row.source_last_updated,
        source_state=pull_request_row.source_state,
        state=pull_request_row.state,
        merge_status=pull_request_row.source_merge_status,
        end_date=pull_request_row.end_date,
        source_branch=pull_request_row.source_branch,
        target_branch=pull_request_row.target_branch,
        source_id=pull_request_row.source_id,
        display_id=pull_request_row.source_display_id,
        deleted_at=pull_request_row.deleted_at,
        source_repository_key=repository_key
    )


def sync_pull_requests(session, repository_key, source_pull_requests):
    if repository_key:
        repository = Repository.find_by_repository_key(session, repository_key)
        repository_id = repository.id
        # create a temp table for pull requests and insert source_pull_requests
        pull_requests_temp = db.temp_table_from(
            pull_requests,
            table_name='pull_requests_temp',
            exclude_columns=[
                pull_requests.c.id,
                pull_requests.c.source_repository_id,
            ],
            extra_columns=[
                Column('source_repository_id', Integer, nullable=True),
            ]
        )
        last_sync = datetime.utcnow()
        pull_requests_temp.create(session.connection(), checkfirst=True)
        for source_prs in source_pull_requests:
            session.connection().execute(
                pull_requests_temp.insert([
                    dict(
                        repository_id=repository_id,
                        key=uuid.uuid4(),
                        last_sync=last_sync,
                        **source_pr
                    )
                    for source_pr in source_prs
                ])
            )

        # Add source and target repo ids

        session.connection().execute(
            pull_requests_temp.update().where(
                repositories.c.source_id == pull_requests_temp.c.source_repository_source_id,
            ).values(
                source_repository_id=repositories.c.id,
            )
        )

        new_and_updated_pull_requests = session.connection().execute(
            select([*pull_requests_temp.columns, pull_requests.c.key.label('current_key'), \
                    repositories.c.key.label('source_repository_key')]).select_from(
                pull_requests_temp.outerjoin(
                    pull_requests,
                    and_(
                        pull_requests_temp.c.repository_id == pull_requests.c.repository_id,
                        pull_requests_temp.c.source_id == pull_requests.c.source_id
                    )
                ).join(
                    repositories, pull_requests_temp.c.source_repository_id == repositories.c.id
                )
            ).where(
                or_(
                    pull_requests_temp.c.source_last_updated > pull_requests.c.source_last_updated,
                    pull_requests.c.key == None
                )
            )
        ).fetchall()

        # Update pull_requests
        upsert = insert(
            pull_requests
        ).from_select(
            [column.name for column in pull_requests_temp.columns],
            select(
                [pull_requests_temp]
            ).where(
                    pull_requests_temp.c.key != None,
            )
        )

        session.connection().execute(
            upsert.on_conflict_do_update(
                index_elements=['repository_id', 'source_id'],
                set_=dict(
                    title=upsert.excluded.title,
                    description=upsert.excluded.description,
                    source_created_at=upsert.excluded.source_created_at,
                    source_last_updated=upsert.excluded.source_last_updated,
                    last_sync=upsert.excluded.last_sync,
                    source_state=upsert.excluded.source_state,
                    state=upsert.excluded.state,
                    source_merge_status=upsert.excluded.source_merge_status,
                    source_merged_at=upsert.excluded.source_merged_at,
                    source_closed_at=upsert.excluded.source_closed_at,
                    end_date=upsert.excluded.end_date,
                    deleted_at=upsert.excluded.deleted_at,
                )
            )
        )

        synced_pull_requests = []
        # NOTE: Had to check for None, as in the case when there are no fetched PRs,
        # new_and_updated_pull_request has entries with all None values
        for pr in new_and_updated_pull_requests:
            if pr.source_id is not None:
                synced_pull_requests.append(
                    pull_request_summary(
                        pr,
                        is_new=pr.current_key is None,
                        repository_key=pr.source_repository_key,
                        key=pr.key if pr.current_key is None else pr.current_key
                    ),
                )
        return dict(
            success=True,
            pull_requests=synced_pull_requests
        )


def get_pull_request_summary(session, pull_request_key):
    pr = PullRequest.find_by_pull_request_key(session, pull_request_key)
    if pr is not None:
        return pull_request_summary(pr, is_new=False, repository_key=pr.repository.key)

    else:
        raise ProcessingException(f"Could not find pull request with key {pull_request_key}")


def get_pull_requests_to_sync_with_analytics(session, before=None, days=1, threshold_minutes=15, limit=100):
    if before is None:
        # by default, we dont sync anything that was updated in the last threshold_minutes minutes
        before = datetime.utcnow() - timedelta(minutes=threshold_minutes)

    # we only sync items that have been out of sync for a certain window. This ensures
    # that we dont keep trying to sync things indefintely if they are in a permanently failed state.
    after = datetime.utcnow() - timedelta(days=1)

    pull_requests_to_sync = [
        dict(
            organization_key=result.organization_key,
            repository_key=result.repository_key,
            pull_request_summary=pull_request_summary(
                result,
                is_new=result.analytics_last_updated is None,
                repository_key=result.repository_key
            )
        )
        for result in
        session.connection().execute(
            select([
                repositories.c.organization_key,
                repositories.c.key.label('repository_key'),
                *pull_requests.columns
            ]).select_from(
                pull_requests.join(
                    repositories, pull_requests.c.repository_id == repositories.c.id
                )
            ).where(
                and_(
                    pull_requests.c.source_last_updated < before,
                    pull_requests.c.source_last_updated >= after,
                    or_(
                        pull_requests.c.analytics_last_updated == None,
                        pull_requests.c.source_last_updated > pull_requests.c.analytics_last_updated
                    )
                )
            ).order_by(
                pull_requests.c.source_last_updated.desc()
            ).limit(
                limit
            )
        ).fetchall()
    ]

    return dict(
        success=True,
        pull_requests=pull_requests_to_sync
    )


def get_pull_requests_to_sync_with_source(session, before, days, limit):
    if before is None:
        before = datetime.utcnow()

    result = [
        dict(
            organization_key=record.organization_key,
            repository_key=record.repository_key,
            pull_request_key=record.pull_request_key,
            source_last_updated=record.source_last_updated
        )

        for record in session.connection().execute(
            select([
                pull_requests.c.key.label('pull_request_key'),
                repositories.c.key.label('repository_key'),
                repositories.c.organization_key.label('organization_key'),
                pull_requests.c.source_last_updated

            ]).select_from(
                pull_requests.join(
                    repositories, pull_requests.c.repository_id == repositories.c.id
                )
            ).where(
                and_(
                    pull_requests.c.state == 'open',
                    repositories.c.public == False,
                    pull_requests.c.source_last_updated < before,
                    func.extract('epoch', datetime.utcnow() - pull_requests.c.source_last_updated) > days * 24 * 3600
                )
            ).order_by(
                pull_requests.c.source_last_updated.desc()
            ).limit(
                limit
            )
        ).fetchall()
    ]
    return dict(
        success=True,
        pull_requests=result
    )


def ack_pull_request_event(session, pull_request_summaries):
    update_stmt = pull_requests.update().where(
        and_(
            pull_requests.c.key == bindparam('pull_request_key'),
            # we dont want the sync dates to be monotonically non-decreasing so ignore
            # acks that go back in time
            or_(
                pull_requests.c.analytics_last_updated == None,
                pull_requests.c.analytics_last_updated < bindparam('analytics_last_updated')
            )
        )
    ).values(
        dict(
            analytics_last_updated=bindparam('analytics_last_updated')
        )
    )
    updated = session.connection().execute(
        update_stmt,
        [
            dict(
                pull_request_key=pr['key'],
                # source_last_updated for this message is timestamp for
                # last time this was synced with analytics
                analytics_last_updated=pr['updated_at']
            )
            for pr in pull_request_summaries
        ]
    ).rowcount

    return dict(
        success=True,
        updated=updated
    )
