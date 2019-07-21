# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import logging
import argh
import time

from sqlalchemy import select, func, and_, bindparam
from polaris.common import db
from polaris.utils.logging import config_logging
from polaris.utils.timer import Timer

from polaris.repos.db.model import Repository, Organization
from polaris.repos.intake.service.messaging import publish

from polaris.repos.db.schema import commits, source_file_versions, source_files

logger = logging.getLogger('polaris.repos.intake.service.resync_commits')


def get_commit_details(session, commits_batch):
    commit_detail_rows = session.connection().execute(
        select([
            commits.c.id,
            commits.c.key,
            commits.c.source_commit_id,
            commits.c.parents,
            commits.c.stats,
            source_file_versions.c.action,
            source_file_versions.c.version_info,
            source_files.c.key.label('source_file_key'),
            source_files.c.name,
            source_files.c.path,
            source_files.c.file_type,
            source_files.c.is_deleted,
            source_files.c.version_count
        ]).select_from(
            commits.outerjoin(
                source_file_versions, source_file_versions.c.commit_id == commits.c.id
            ).outerjoin(
                source_files, source_file_versions.c.source_file_id == source_files.c.id
            )
        ).where(
            commits.c.id.in_([commit.id for commit in commits_batch])
        ).order_by(commits.c.id)
    ).fetchall()

    current_commit_id = None
    current_commit = None
    commit_details_batch = []
    for commit_detail in commit_detail_rows:
        if commit_detail.id != current_commit_id:
            current_commit_id = commit_detail.id
            current_commit = dict(
                key=commit_detail.key,
                source_commit_id=commit_detail.source_commit_id,
                stats=commit_detail.stats,
                parents=commit_detail.parents,
                source_files=[]
            )
            commit_details_batch.append(current_commit)
        if commit_detail.source_file_key is not None:
            current_commit['source_files'].append(
                dict(
                    key=commit_detail.source_file_key,
                    name=commit_detail.name,
                    path=commit_detail.path,
                    file_type=commit_detail.file_type if commit_detail.file_type is not None else " ",
                    is_deleted=commit_detail.is_deleted,
                    stats=commit_detail.version_info,
                    action=commit_detail.action,
                    version_count=commit_detail.version_count
                )
            )
    return commit_details_batch


def send_for_repository(repository_key, repository=None, organization_key=None, batch_size=None, session=None):
    with db.orm_session(session) as session:
        if repository is None:
            repository = Repository.find_by_repository_key(session, repository_key)
        if organization_key is None:
            organization_key = repository.organization.organization_key if repository.organization else None

        with Timer(
                action=f"Send commit details for repository {repository.name}",
                loglevel=logging.INFO,
                use_logger=logger
        ):
            if batch_size is None:
                batch_size = 1000

            total = session.connection().execute(
                select([
                    func.count(commits.c.id)
                ]).where(
                    and_(
                        commits.c.repository_id == repository.id,
                        commits.c.sync_state == 1,
                        commits.c.analytics_details_synced_at == None
                    )
                )
            ).scalar()
            if total > 0:
                logger.info(f"{total} commits to sync")

            synced = 0
            max_id = 0
            while synced < total:
                commits_batch = session.connection().execute(
                    select([
                        commits.c.id
                    ]).where(
                        and_(
                            commits.c.repository_id == repository.id,
                            commits.c.sync_state == 1,
                            commits.c.analytics_details_synced_at == None,
                            commits.c.id > bindparam('max_id')
                        )
                    ).order_by(
                        commits.c.id
                    ).limit(
                        batch_size
                    ),
                    dict(max_id=max_id)
                    ).fetchall()

                if len(commits_batch) > 0:
                    max_id = commits_batch[len(commits_batch)-1].id

                commit_details_batch = get_commit_details(session, commits_batch)
                synced = synced + len(commit_details_batch)

                if len(commit_details_batch) > 0:

                    publish.publish_commit_details_imported(
                        dict(
                            organization_key=organization_key,
                            repository_name=repository.name,
                            repository_key=repository.key,
                            commit_details=commit_details_batch
                        )
                    )
                    time.sleep(1)
                else:
                    break

            logger.info(f'Synced {synced} commit details for repository {repository.name}')
        return total


def send_for_organization(organization_key, batch_size):
    with db.orm_session() as session:
        organization = Organization.find_by_organization_key(session, organization_key)
        with Timer(
                action=f"send commit details for organization {organization.name}",
                use_logger=logger,
                loglevel=logging.INFO
        ):
            total = 0
            repositories = Repository.find_by_organization_key(session, organization_key)
            for repository in repositories:
                total = total + send_for_repository(
                    repository_key=repository.key,
                    repository=repository,
                    organization_key=organization.organization_key,
                    batch_size=batch_size,
                    session=session
                )

            logger.info(f"{total} commits synced for organization {organization.name} ")


def commit_details_imported(organization_key=None, repository_key=None, batch_size=100):
    if repository_key is not None:
        send_for_repository(repository_key, batch_size=batch_size)
    elif organization_key is not None:
        send_for_organization(organization_key, batch_size)
    else:
        raise Exception("At least one of organization_key or repository_key must be specified")
