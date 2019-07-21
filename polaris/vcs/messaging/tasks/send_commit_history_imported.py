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

from polaris.repos.db.schema import commits, contributor_aliases

logger = logging.getLogger('polaris.repos.intake.service.resync_commits')

def resolve_new_aliases(commit_batch, contributor_aliases_cache):
    new_aliases = []
    for commit in commit_batch:
        if commit.committer_alias_key not in contributor_aliases_cache:
            contributor_aliases_cache.add(commit.committer_alias_key)
            new_aliases.append(
                dict(
                    name=commit.committer_name,
                    alias=commit.committer_alias,
                    key=commit.committer_alias_key
                )
            )

        if commit.author_alias_key not in contributor_aliases_cache:
            contributor_aliases_cache.add(commit.author_alias_key)
            new_aliases.append(
                dict(
                    name=commit.author_name,
                    alias=commit.author_alias,
                    key=commit.author_alias_key
                )
            )

    return new_aliases


def send_for_repository(repository_key, repository=None, organization_key=None, batch_size=None, session=None):
    with db.orm_session(session) as session:
        if repository is None:
            repository = Repository.find_by_repository_key(session, repository_key)
        if organization_key is None:
            organization_key = repository.organization.organization_key if repository.organization else None

        with Timer(
                action=f"Sync commit history for repository {repository.name}",
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
                        commits.c.analytics_commit_synced_at == None
                    )
                )
            ).scalar()
            if total > 0:
                logger.info(f"{total} commits to sync")

            committers = contributor_aliases.alias('committer')
            authors = contributor_aliases.alias('author')

            contributor_alias_cache = set()

            batch = 0
            synced = 0
            max_id = 0
            while synced < total:
                commit_batch = session.connection().execute(
                    select([
                        *commits.columns,
                        committers.c.alias.label('committer_alias'),
                        committers.c.display_name.label('committer_name'),
                        committers.c.key.label('committer_alias_key'),
                        authors.c.alias.label('author_alias'),
                        authors.c.display_name.label('author_name'),
                        authors.c.key.label('author_alias_key')
                    ]).select_from(
                        commits.join(
                            committers, committers.c.id == commits.c.committer_alias_id
                        ).join(
                            authors, authors.c.id == commits.c.author_alias_id
                        )
                    ).where(
                        and_(
                            commits.c.repository_id == repository.id,
                            commits.c.analytics_commit_synced_at == None,
                            commits.c.id > bindparam('max_id')
                        )
                    ).order_by(
                        commits.c.id
                    ).limit(
                        batch_size
                    ),
                    dict(max_id=max_id)
                ).fetchall()
                if len(commit_batch) > 0:
                    max_id = commit_batch[len(commit_batch)-1].id

                    if batch > 0:
                        logger.info(f"Processing batch: {batch}")

                    new_contributor_aliases = resolve_new_aliases(commit_batch, contributor_alias_cache)

                    publish.publish_commit_history_imported(
                        dict(
                            organization_key=organization_key,
                            repository_name=repository.name,
                            repository_key=repository.key,
                            total_commits=len(commit_batch),
                            new_commits=commit_batch,
                            new_contributors=new_contributor_aliases
                        )
                    )
                    synced = synced + len(commit_batch)
                    time.sleep(2)
                else:
                    break

                batch = batch + 1

            logger.info(f'Synced {synced} commits for repository {repository.name}')
        return total


def send_for_organization(organization_key, batch_size):
    with db.orm_session() as session:
        organization = Organization.find_by_organization_key(session, organization_key)
        with Timer(
            action=f"sync unacked commits for organization {organization.name}",
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


def commit_history_imported(organization_key=None, repository_key=None, batch_size=1000):
    if repository_key is not None:
        send_for_repository(repository_key, batch_size=batch_size)
    elif organization_key is not None:
        send_for_organization(organization_key, batch_size)
    else:
        raise Exception("At least one of organization_key or repository_key must be specified")


