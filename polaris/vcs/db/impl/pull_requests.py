# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
import logging
from datetime import datetime
from polaris.repos.db.model import Repository, pull_requests
from polaris.repos.db.schema import branches
from polaris.common import db

from sqlalchemy import and_


log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def import_pull_requests(session, organization_key, repository_key, source_pull_requests):
    if organization_key and repository_key:
        repository = Repository.find_by_repository_key(repository_key)
        source_repo_id = repository.source_id
        # create a temp table for pull requests and insert source_pull_requests
        pull_requests_temp = db.temp_table_from(
            pull_requests,
            table_name='pull_requests_temp',
            exclude_columns=[
                pull_requests.c.id,
                pull_requests.c.last_sync,
                pull_requests.c.deleted_at
            ]
        )

        pull_requests_temp.create(session.connection(), checkfirst=True)
        session.connection().execute(
            pull_requests_temp.insert([
                dict(
                    key=uuid.uuid4(),
                    last_updated=datetime.utcnow(),
                    **source_pr
                )
                for source_pr in source_pull_requests
            ])
        )
        # Add branch and commit information
        source_branches = branches.alias('source_branches')
        target_branches = branches.alias('target_branches')
        pull_requests_before_insert = session.connection().execute(
            select([
                *pull_requests_temp.columns,
                source_branches.c.id.label('source_branch_id'),
                target_branches.c.id.label('target_branch_id'),
                source_branches.c.latest_commit.label('source_branch_latest_commit'),
                target_branches.c.latest_commit.label('target_branch_latest_commit'),
                # FIXME: Not sure if next_seq_no is what we intend to store as latest seq no. Discuss.
                source_branches.c.next_seq_no.label('source_branch_latest_seq_no'),
                target_branches.c.next_seq_no.label('target_branch_latest_seq_no')
            ]).select_from(
                pull_requests_temp.join(
                    source_branches,
                    and_(
                        source_branches.c.repository_id == pull_requests_temp.c.repository_id,
                        source_branches.c.name == pull_requests_temp.c.source_branch
                    )
                ).join(
                    target_branches,
                    and_(
                        target_branches.c.repository_id == pull_requests_temp.c.repository_id,
                        target_branches.c.name == pull_requests_temp.c.target_branch
                    )
                )
            )
        ).cte('pull_requests_before_insert')
        # insert into the repos.pull_requests table

        session.connection().execute(
            pull_requests.insert().from_select([
                'key',
                'source_pull_request_id',
                'title',
                'description',
                'web_url',
                'source_created_at',
                'source_last_updated',
                'last_updated',
                'source_state',
                'source_merge_status',
                'source_merged_at',
                'source_branch',
                'source_branch_id',
                'source_branch_latest_commit',
                'source_branch_latest_seq_no',
                'target_branch',
                'target_branch_id',
                'target_branch_latest_commit',
                'target_branch_latest_seq_no',
                'source_repository_id',
                'target_repository_id'
            ], select(
                [
                    pull_requests_before_insert.c.key,
                    pull_requests_before_insert.c.source_pull_request_id,
                    pull_requests_before_insert.c.title,
                    pull_requests_before_insert.c.description,
                    pull_requests_before_insert.c.web_url,
                    pull_requests_before_insert.c.source_created_at,
                    pull_requests_before_insert.c.source_last_updated,
                    pull_requests_before_insert.c.last_updated,
                    pull_requests_before_insert.c.source_state,
                    pull_requests_before_insert.c.source_merge_status,
                    pull_requests_before_insert.c.source_merged_at,
                    pull_requests_before_insert.c.source_branch,
                    pull_requests_before_insert.c.source_branch_id,
                    pull_requests_before_insert.c.source_branch_latest_commit,
                    pull_requests_before_insert.c.source_branch_latest_seq_no,
                    pull_requests_before_insert.c.target_branch,
                    pull_requests_before_insert.c.target_branch_id,
                    pull_requests_before_insert.c.target_branch_latest_commit,
                    pull_requests_before_insert.c.target_branch_latest_seq_no,
                    pull_requests_before_insert.c.source_repository_id,
                    pull_requests_before_insert.c.target_repository_id
                ]
            ).where(
                # FIXME: Clarify the condition when we want to keep a pull request
                pull_requests_before_insert.c.source_repository_id == pull_requests_before_insert.c.target_repository_id
            )
            )
        )

        return dict(
            success=True
        )