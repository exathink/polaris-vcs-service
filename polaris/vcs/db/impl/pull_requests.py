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

from sqlalchemy import select, and_


log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def import_pull_requests(session, organization_key, repository_key, source_pull_requests):
    if organization_key and repository_key:
        repository = Repository.find_by_repository_key(repository_key)
        repository_id = repository.id
        # create a temp table for pull requests and insert source_pull_requests
        pull_requests_temp = db.temp_table_from(
            pull_requests,
            table_name='pull_requests_temp',
            exclude_columns=[
                pull_requests.c.id,
                pull_requests.c.last_sync,
                pull_requests.c.deleted_at,
                pull_requests.c.source_repository_id,
                pull_requests.c.target_repository_id,
                pull_requests.c.source_branch_latest_commit,
                pull_requests.c.source_branch_latest_seq_no
            ]
        )

        pull_requests_temp.create(session.connection(), checkfirst=True)
        session.connection().execute(
            pull_requests_temp.insert([
                dict(
                    repository_id=repository_id,
                    key=uuid.uuid4(),
                    last_updated=datetime.utcnow(),
                    **source_pr
                )
                for source_pr in source_pull_requests
            ])
        )
        # Add branch and commit information
        source_branch = branches.alias('source_branch')
        target_branch = branches.alias('target_branch')
        source_repo = repository.alias('source_repo')
        target_repo = repository.alias('target_repo')
        pull_requests_before_insert = select([
                *pull_requests_temp.columns,
                source_repo.c.id('source_repository_id'),
                target_repo.c.id('target_repository_id'),
                source_branch.c.id.label('source_branch_id'),
                target_branch.c.id.label('target_branch_id'),
            # TODO: Need to join with branch_commits and commits tables
            # to find the latest commit and seq no
            # Commit here is the source commit hash
                source_branch.c.latest_commit.label('source_branch_latest_commit'),
                target_branch.c.latest_commit.label('target_branch_latest_commit'),
            (source_branch.c.next_seq_no-1).label('source_branch_latest_seq_no'),
            (target_branch.c.next_seq_no-1).label('target_branch_latest_seq_no')
            ]).select_from(
                pull_requests_temp.join(
                    source_repo, source_repo.c.source_id == pull_requests_temp.c.source_repository_source_id
                ).join(
                    source_branch,
                    and_(
                        source_branch.c.repository_id == source_repo.c.id,
                        source_branch.c.name == pull_requests_temp.c.source_branch
                    )
                ).join(
                    target_repo, target_repo.c.source_id == pull_requests_temp.c.target_repository_source_id
                ).join(
                    target_branch,
                    and_(
                        target_branch.c.repository_id == target_repo.c.id,
                        target_branch.c.name == pull_requests_temp.c.target_branch
                    )
                )
            ).cte('pull_requests_before_insert')

        # To find latest commit and latest seq no
        #
        # insert into the repos.pull_requests table

        # TODO: Convert to upsert
        session.connection().execute(
            pull_requests.insert().from_select([
                'repository_id',
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
                'source_repository_source_id',
                'target_repository_source_id',
                'source_repository_id',
                'target_repository_id'
            ], select(
                [
                    pull_requests_before_insert.c.repository_id,
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
                    pull_requests_before_insert.c.source_repository_source_id,
                    pull_requests_before_insert.c.target_repository_source_id,
                    pull_requests_before_insert.c.source_repository_id,
                    pull_requests_before_insert.c.target_repository_id
                ]
            )
            )
        )

        return dict(
            success=True
        )