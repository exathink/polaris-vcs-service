# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
import logging
from datetime import datetime
from polaris.repos.db.model import Repository, pull_requests, repositories
from polaris.common import db
from sqlalchemy import select, and_, Column, Integer
from sqlalchemy.dialects.postgresql import insert

log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


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

        pull_requests_before_insert = session.connection().execute(
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
            )
        ).fetchall()

        # Update pull_requests
        upsert = insert(pull_requests).from_select(
            [column.name for column in pull_requests_temp.columns],
            select([pull_requests_temp]).where(pull_requests_temp.c.key != None)
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
                    deleted_at=upsert.excluded.deleted_at,
                )
            )
        )

        synced_pull_requests = []
        # NOTE: Had to check for None, as in the case when there are no fetched PRs,
        # pull_requests_before_insert has entries with all None values
        for pr in pull_requests_before_insert:
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
                        merged_at=pr.source_merged_at,
                        closed_at=pr.source_closed_at,
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
