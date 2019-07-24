# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert

from polaris.common import db
from polaris.repos.db.schema import repositories, RepositoryImportState
from polaris.utils.exceptions import ProcessingException


def sync_repositories(session, organization_key, source_repositories):
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
                repositories.c.failures_since_last_checked
            ]

        )
        repositories_temp.create(session.connection(), checkfirst=True)
        session.connection().execute(
            repositories_temp.insert([
                dict(
                    key=uuid.uuid4(),
                    organization_key=organization_key,
                    import_state=RepositoryImportState.IMPORT_DISABLED,
                    import_ready_state=RepositoryImportState.IMPORT_READY,
                    update_ready_state=RepositoryImportState.UPDATE_READY,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
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
                )
                for repository in repositories_before_insert
            ])

    else:
        raise ProcessingException(f"Connector {connector.key} must specify an organization key "
                                  f"to import repositories")
