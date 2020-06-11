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
from polaris.repos.db.schema import branches
from polaris.common import db

from sqlalchemy import select, insert, and_, Column, String, Integer

log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def sync_pull_requests(session, organization_key, repository_key, source_pull_requests):
    if organization_key and repository_key:
        repository = Repository.find_by_repository_key(repository_key)
        repository_id = repository.id
        # create a temp table for pull requests and insert source_pull_requests
        pull_requests_temp = db.temp_table_from(
            pull_requests,
            table_name='pull_requests_temp',
            exclude_columns=[
                pull_requests.c.id,
                pull_requests.c.source_repository_id,
                pull_requests.c.target_repository_id,
                pull_requests.c.source_branch_latest_commit,
                pull_requests.c.source_branch_latest_seq_no
            ],
            extra_columns=[
                Column('source_repository_id', Integer, nullable=True),
                Column('target_repository_id', Integer, nullable=True),
                Column('source_branch_latest_commit', String, nullable=True),
                Column('source_branch_latest_seq_no', Integer, nullable=True)
            ]
        )
        last_sync = datetime.utcnow()
        pull_requests_temp.create(session.connection(), checkfirst=True)
        session.connection().execute(
            pull_requests_temp.insert([
                dict(
                    repository_id=repository_id,
                    key=uuid.uuid4(),
                    last_sync=last_sync,
                    **source_pr
                )
                for source_pr in source_pull_requests
            ])
        )

        # Add source and target repo ids
        # source_repos = repositories.alias('source_repos')
        # target_repos = repositories.alias('target_repos')

        session.connection().execute(
            pull_requests_temp.update().where(
                repositories.c.source_id == pull_requests_temp.c.source_repository_source_id,
            ).values(
                source_repository_id=repositories.c.id,
            )
        )

        session.connection().execute(
            pull_requests_temp.update().where(
                repositories.c.source_id == pull_requests_temp.c.target_repository_source_id
            ).values(
                target_repository_id=repositories.c.id
            )
        )

        # Resolving branches information
        # FIXME: Adding the latest commit and seq no from branches to fill in non-nullable fields \
        #  Will need to do more when handling commits properly
        source_branch = branches.alias('source_branch')
        target_branch = branches.alias('target_branch')

        session.connection().execute(
            pull_requests_temp.update().where(
                and_(
                    source_branch.c.repository_id == pull_requests_temp.c.source_repository_id,
                    source_branch.c.name == pull_requests_temp.c.source_branch,
                    target_branch.c.repository_id == pull_requests_temp.c.target_respository_id,
                    target_branch.c.name == pull_requests_temp.c.target_branch
                )
            ).values(
                source_branch_id=source_branch.c.id,
                target_branch_id=target_branch.c.id,
                source_branch_latest_commit=source_branch.c.latest_commit,
                source_branch_latest_seq_no=source_branch.c.next_seq_no - 1
            )
        )

        pull_requests_before_insert = session.connection().execute(
            select([*pull_requests_temp.columns, pull_requests.c.key.label('current_key')]).select_from(
                pull_requests_temp.outerjoin(
                    pull_requests,
                    and_(
                        pull_requests_temp.c.repository_id == pull_requests.c.repository_id,
                        pull_requests_temp.c.source_id == pull_requests.c.source_id
                    )
                )
            )
        ).fetchall()

        # Update pull_requests
        upsert = insert(pull_requests).from_select(
            [column.name for column in pull_requests_temp.columns],
            select([pull_requests_temp])
        )

        session.connection().execute(
            upsert.on_conflict_do_update(
                index_elements=['repository_id', 'source_id'],
                set=dict(
                    key=upsert.excluded.key,
                    title=upsert.excluded.title,
                    description=upsert.excluded.description,
                    web_url=upsert.excluded.web_url,
                    source_created_at=upsert.excluded.source_created_at,
                    source_last_updated=upsert.excluded.source_last_updated,
                    last_updated=upsert.excluded.last_updated,
                    source_state=upsert.excluded.source_state,
                    source_merge_status=upsert.excluded.source_merge_status,
                    source_merged_at=upsert.excluded.source_merged_at,
                    source_branch=upsert.excluded.source_branch,
                    source_branch_id=upsert.excluded.source_branch_id,
                    source_branch_latest_commit=upsert.excluded.source_branch_latest_commit,
                    source_branch_latest_seq_no=upsert.excluded.source_branch_latest_seq_no,
                    target_branch=upsert.excluded.target_branch,
                    target_branch_id=upsert.excluded.target_branch_id,
                    source_repository_source_id=upsert.excluded.source_repository_source_id,
                    target_repository_source_id=upsert.excluded.target_repository_source_id,
                    source_repository_id=upsert.excluded.source_repository_id,
                    target_repository_id=upsert.excluded.target_repository_id
                )
            )
        )

        return [
            dict(
                is_new=pr.current_key is None,
                key=pr.key if pr.current_key is None else pr.current_key,
                title=pr.title,
                description=pr.description,
                web_url=pr.web_url,
                source_created_at=pr.source_created_at,
                source_last_updated=pr.source_last_updated,
                last_updated=pr.last_updated,
                source_state=pr.source_state,
                source_merge_status=pr.source_merge_status,
                source_merged_at=pr.source_merged_at,
                source_branch=pr.source_branch,
                source_branch_id=pr.source_branch_id,
                source_branch_latest_commit=pr.source_branch_latest_commit,
                source_branch_latest_seq_no=pr.source_branch_latest_seq_no,
                target_branch=pr.target_branch,
                target_branch_id=pr.target_branch_id,
                source_repository_source_id=pr.source_repository_source_id,
                target_repository_source_id=pr.target_repository_source_id,
                source_repository_id=pr.source_repository_id,
                target_repository_id=pr.target_repository_id,
                source_id=pr.source_id
            )
            for pr in pull_requests_before_insert
        ]
