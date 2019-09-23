# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
import logging

from datetime import datetime

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert

from polaris.common import db
from polaris.repos.db.model import repositories, Repository
from polaris.repos.db.schema import RepositoryImportState
from polaris.utils.exceptions import ProcessingException
from polaris.common.db import row_proxy_to_dict
from polaris.utils.collections import dict_merge

log = logging.getLogger('polaris.vcs.db.impl.repositories')


def sync_repositories(session, organization_key, connector_key, source_repositories):
    if organization_key is not None:
        repositories_temp = db.temp_table_from(
            repositories,
            table_name='repositories_temp',
            exclude_columns=[
                repositories.c.id,
                repositories.c.last_checked,
                repositories.c.last_imported,
                repositories.c.commit_count,
                repositories.c.earliest_commit,
                repositories.c.latest_commit,
                repositories.c.failures_since_last_checked,
                repositories.c.organization_id
            ]

        )
        repositories_temp.create(session.connection(), checkfirst=True)
        session.connection().execute(
            repositories_temp.insert([
                dict(
                    key=uuid.uuid4(),
                    organization_key=organization_key,
                    connector_key=connector_key,
                    import_state=RepositoryImportState.IMPORT_DISABLED,
                    import_ready_state=RepositoryImportState.IMPORT_READY,
                    update_ready_state=RepositoryImportState.UPDATE_READY,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    source_data={},
                    **source_repo
                )
                for source_repo in source_repositories
            ])
        )
        repositories_before_insert = session.connection().execute(
            select([*repositories_temp.columns, repositories.c.key.label('current_key')]).select_from(
                repositories_temp.outerjoin(
                    repositories,
                    and_(
                        repositories_temp.c.connector_key == repositories.c.connector_key,
                        repositories_temp.c.source_id == repositories.c.source_id
                    )
                )
            )
        ).fetchall()

        upsert = insert(repositories).from_select(
            [column.name for column in repositories_temp.columns],
            select([repositories_temp])
        )

        session.connection().execute(
            upsert.on_conflict_do_update(
                index_elements=['connector_key', 'source_id'],
                set_=dict(
                    name=upsert.excluded.name,
                    description=upsert.excluded.description,
                    url=upsert.excluded.url,
                    public=upsert.excluded.public,
                    properties=upsert.excluded.properties,
                    updated_at=upsert.excluded.updated_at,
                )
            )
        )

        return dict(
            success=True,
            repositories=[
                dict(
                    is_new=repository.current_key is None,
                    key=repository.key if repository.current_key is None else repository.current_key,
                    connector_key=repository.connector_key,
                    integration_type=repository.integration_type,
                    source_id=repository.source_id,
                    url=repository.url,
                    name=repository.name,
                    description=repository.description,
                    public=repository.public,
                    organization_key=organization_key,
                )
                for repository in repositories_before_insert
            ])

    else:
        raise ProcessingException(f"Connector {connector.key} must specify an organization key "
                                  f"to import repositories")


def repository_summary_columns():
    return [
        repositories.c.key,
        repositories.c.name,
        repositories.c.description,
        repositories.c.url,
        repositories.c.integration_type,
        repositories.c.public,
        repositories.c.source_id,
        repositories.c.organization_key,
        repositories.c.connector_key
    ]


def import_repositories(session, organization_key, repository_keys):
    repository_info = session.connection().execute(
        select(
            repository_summary_columns()
        ).where(
            and_(
                repositories.c.organization_key == organization_key,
                repositories.c.key.in_(
                    repository_keys
                )
            )
        )
    ).fetchall()

    if len(repository_info) == len(repository_keys):
        # Mark the repositories ready for import
        session.connection().execute(
            repositories.update().values(
                import_state=RepositoryImportState.IMPORT_READY
            ).where(
                and_(
                    repositories.c.organization_key == organization_key,
                    repositories.c.key.in_(repository_keys)
                )
            )
        )
    else:
        raise ProcessingException(f'Not all repositories in the '
                                  f'specified list could be found in the current organization: '
                                  f'Expected {len(repository_keys)}'
                                  f'Found: {len(repository_info)}')
    return dict(
        success=True,
        organization_key=organization_key,
        repositories=[
            row_proxy_to_dict(repository)
            for repository in repository_info
        ]
    )


def register_webhook(session, organization_key, repository_key, webhook_info):
    repo = Repository.find_by_repository_key(session, repository_key)
    if repo is not None:
        log.info(f'Registering webhook for organization {organization_key} Repository {repo.name}')
        source_data = dict(repo.source_data)
        source_data['webhooks'] = dict_merge(source_data.get('webhooks', {}), webhook_info['webhooks'])
        repo.source_data = source_data
        if 'repository_push' in webhook_info['webhooks']:
            repo.polling = False
    else:
        raise ProcessingException(f"Could not find repository with key {repository_key}")


def handle_remote_repository_push(session, connector_key, repository_source_id):
    repo = Repository.find_by_connector_key_source_id(session, connector_key, repository_source_id)
    if repo is not None:
        log.info(f'Received repository push for organization {repo.organization_key} Repository {repo.name}')
        if repo.import_state == RepositoryImportState.CHECK_FOR_UPDATES:
            repo.import_state = RepositoryImportState.UPDATE_READY

        return dict(
            success=True,
            organization_key=repo.organization_key,
            repository_key=repo.key,
        )
    else:
        raise ProcessingException(f"Could not find repository with key {repository_key}")



