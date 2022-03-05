# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
import logging

from datetime import datetime

from sqlalchemy import select, and_, Column, Boolean
from sqlalchemy.dialects.postgresql import insert

from polaris.common import db
from polaris.repos.db.model import repositories, Repository
from polaris.repos.db.schema import RepositoryImportState
from polaris.utils.exceptions import ProcessingException
from polaris.utils.collections import find
from polaris.common.db import row_proxy_to_dict

log = logging.getLogger('polaris.vcs.db.impl.repositories')


def sync_repositories(session, organization_key, connector_key, source_repositories):
    if organization_key is not None:
        if len(source_repositories) > 0:
            extra_columns = [
                    Column('exists_in_organization', Boolean())
            ]
            extra_column_names = [column.name for column in extra_columns]

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
                ],
                extra_columns=extra_columns
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
                        exists_in_organization=False,
                        **source_repo
                    )
                    for source_repo in source_repositories
                ])
            )
            # We need to prevent multiple copies of the same repo being imported into the same
            # organization under different connectors. Ideally we would do this via a database constraint, but for legacy reasons,
            # we used to allow this early on, and it is hard to clean up old data without affecting active
            # accounts, so we are checking explicitly for this here.

            exists_in_organization = select([repositories_temp.c.key]).select_from(
                repositories_temp.join(
                    repositories,
                    and_(
                        repositories_temp.c.organization_key == repositories.c.organization_key,
                        repositories_temp.c.source_id == repositories.c.source_id,
                        # note: this check is important, otherwise the operation will not update
                        # existing repos under the same connector.
                        repositories_temp.c.connector_key != repositories.c.connector_key
                    )
                )
            ).cte()

            session.connection().execute(
                repositories_temp.update().values(
                    exists_in_organization=True
                ).where(
                    repositories_temp.c.key == exists_in_organization.c.key
                )
            )
            # This list will be used to return values from the call.
            repositories_to_insert = session.connection().execute(
                select([*repositories_temp.columns, repositories.c.key.label('current_key')]).select_from(
                    repositories_temp.outerjoin(
                        repositories,
                        and_(
                            repositories_temp.c.connector_key == repositories.c.connector_key,
                            repositories_temp.c.source_id == repositories.c.source_id
                        )
                    )
                ).where(
                    repositories_temp.c.exists_in_organization == False
                )
            ).fetchall()

            repositories_columns = [column for column in repositories_temp.columns if column.name not in extra_column_names]
            upsert = insert(repositories).from_select(
                [column.name for column in repositories_columns],
                select(repositories_columns).where(
                    repositories_temp.c.exists_in_organization == False
                )
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
                    for repository in repositories_to_insert
                ])
        else:
            return dict(
                success=True,
                repositories=[]
            )

    else:
        raise ProcessingException(f"Connector {connector_key} must specify an organization key "
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


def register_webhooks(session, repository_key, webhook_info):
    # Replaces active webhook with the latest registered webhook.
    # Moves old active webhook to inactive webhooks
    # Deletes inactive webhook ids which are passed in webhook info and present in source_data
    repo = Repository.find_by_repository_key(session, repository_key)
    if repo is not None:
        log.info(f'Registering webhook for repository {repo.name}')
        source_data = dict(repo.source_data)
        if webhook_info['active_webhook']:
            if source_data.get('active_webhook'):
                inactive_webhooks = source_data.get('inactive_webhooks', [])
                inactive_webhooks.append(source_data.get('active_webhook'))
                source_data['inactive_webhooks'] = inactive_webhooks
            source_data['active_webhook'] = webhook_info['active_webhook']
        for wid in webhook_info['deleted_webhooks']:
            if source_data.get('inactive_webhooks') and wid in source_data.get('inactive_webhooks'):
                source_data['inactive_webhooks'].remove(wid)
        if source_data.get('webhooks'):
            del source_data['webhooks']
        repo.source_data = source_data

        return dict(
            success=True,
            repository_key=repository_key
        )
    else:
        raise ProcessingException(f"Could not find repository with key {repository_key}")


def get_registered_webhooks(session, repository_key):
    repo = Repository.find_by_repository_key(session, repository_key)
    if repo is not None:
        log.info(f'Getting registered webhooks for repository {repo.name}')
        source_data = dict(repo.source_data)
        registered_webhooks = []
        if source_data.get('active_webhook'):
            registered_webhooks.extend(source_data.get('inactive_webhooks', []))
            registered_webhooks.append(source_data.get('active_webhook'))
        return dict(
            success=True,
            repository_key=repository_key,
            registered_webhooks=registered_webhooks
        )
    else:
        raise ProcessingException(f"Could not find repository with key {repository_key}")


def handle_remote_repository_push(session, connector_key, repository_source_id):
    repo = Repository.find_by_connector_key_source_id(session, connector_key, repository_source_id)
    if repo is not None:
        if repo.import_state != RepositoryImportState.IMPORT_DISABLED:
            log.info(f'Received repository push for organization {repo.organization_key} Repository {repo.name}')
            if repo.import_state == RepositoryImportState.CHECK_FOR_UPDATES:
                repo.import_state = RepositoryImportState.UPDATE_READY

            return dict(
                success=True,
                organization_key=repo.organization_key,
                repository_key=repo.key,
            )

    # if the repo does not exist (ie it has not been fetched) or the repo is not in an imported state
    # we return false. In all other cases we consider the push successfully handled.
    return dict(
        success=False
    )
