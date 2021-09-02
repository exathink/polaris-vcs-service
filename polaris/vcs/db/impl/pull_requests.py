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
from sqlalchemy import select, and_, or_, Column, Integer, cast, func, Interval
from sqlalchemy.dialects.postgresql import insert

from polaris.utils.exceptions import ProcessingException

log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def find_pull_request(session, pull_request_key):
    return PullRequest.find_by_pull_request_key(session, pull_request_key)


def get_pull_requests_to_sync(session, before, days, limit):
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
                    func.extract('epoch', datetime.utcnow() - pull_requests.c.source_last_updated) > days*24*3600
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
                    dict(
                        is_new=pr.current_key is None,
                        key=pr.key if pr.current_key is None else pr.current_key,
                        title=pr.title,
                        description=pr.description,
                        web_url=pr.web_url,
                        created_at=pr.source_created_at,
                        updated_at=pr.source_last_updated,
                        source_state=pr.source_state,
                        state=pr.state,
                        merge_status=pr.source_merge_status,
                        end_date=pr.end_date,
                        source_branch=pr.source_branch,
                        target_branch=pr.target_branch,
                        source_id=pr.source_id,
                        display_id=pr.source_display_id,
                        deleted_at=pr.deleted_at,
                        source_repository_key=pr.source_repository_key
                    )
                )
        return dict(
            success=True,
            pull_requests=synced_pull_requests
        )


def get_pull_request_summary(session, pull_request_key):
    pr = PullRequest.find_by_pull_request_key(session, pull_request_key)
    if pr is not None:
        return dict(
            is_new=False,
            key=pr.key,
            title=pr.title,
            description=pr.description,
            web_url=pr.web_url,
            created_at=pr.source_created_at,
            updated_at=pr.source_last_updated,
            source_state=pr.source_state,
            state=pr.state,
            merge_status=pr.source_merge_status,
            end_date=pr.end_date,
            source_branch=pr.source_branch,
            target_branch=pr.target_branch,
            source_id=pr.source_id,
            display_id=pr.source_display_id,
            deleted_at=pr.deleted_at,
            source_repository_key=pr.repository.key
        )

    else:
        raise ProcessingException(f"Could not find pull request with key {pull_request_key}")
