# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common.enums import RepositoryImportMode
from polaris.repos.db.schema import RepositoryImportState
from sqlalchemy import case

def repository_info_columns(repositories):
    return [
        repositories.c.url,
        repositories.c.description,
        repositories.c.integration_type,
        repositories.c.public,
        case([
                (repositories.c.import_state == RepositoryImportState.IMPORT_DISABLED, 'disabled'),
                (repositories.c.import_state == RepositoryImportState.IMPORT_READY, 'import queued'),
                (repositories.c.import_state == RepositoryImportState.IMPORT_SMALL_READY, 'import queued'),
                (repositories.c.import_state == RepositoryImportState.IMPORT_PENDING, 'importing'),
                (repositories.c.import_state == RepositoryImportState.IMPORT_FAILED, 'import failed'),
                (repositories.c.import_state == RepositoryImportState.IMPORT_TIMED_OUT, 'import timed out'),
                (repositories.c.import_state == RepositoryImportState.CHECK_FOR_UPDATES, 'polling for updates'),
                (repositories.c.import_state == RepositoryImportState.SYNC_FAILED, 'polling failed'),
                (repositories.c.import_state == RepositoryImportState.UPDATE_READY, 'update queued'),
                (repositories.c.import_state == RepositoryImportState.UPDATE_PENDING, 'updating'),
                (repositories.c.import_state == RepositoryImportState.UPDATE_FAILED, 'update failed'),
                (repositories.c.import_state == RepositoryImportState.UPDATE_LARGE_READY, 'update queued'),
                (repositories.c.import_state == RepositoryImportState.UPDATE_TIMED_OUT, 'update timed out'),
            ]
        ).label('import_state'),
        case(
            [
                (repositories.c.commit_count == None, 0)
            ],
            else_=repositories.c.commit_count
        ).label('commit_count')
    ]


def apply_filters(repositories, query, **kwargs):
    if 'unimportedOnly' in kwargs and kwargs['unimportedOnly']:
        query = query.where(
            repositories.c.import_state == RepositoryImportState.IMPORT_DISABLED
        )

    if 'importMode' in kwargs:
        query = filter_by_import_state(query, kwargs['importMode'], repositories)

    if 'keys' in kwargs:
        query = query.where(
            repositories.c.key.in_(kwargs['keys'])
        )

    return query


def filter_by_import_state(query, import_mode, repositories):
    if import_mode == RepositoryImportMode.importing.value:
        return query.where(
            repositories.c.import_state.in_(
                [
                    RepositoryImportState.IMPORT_READY,
                    RepositoryImportState.IMPORT_PENDING,
                    RepositoryImportState.IMPORT_SMALL_READY,
                    RepositoryImportState.IMPORT_TIMED_OUT,
                    RepositoryImportState.IMPORT_FAILED,
                ]
            )
        )
    elif import_mode == RepositoryImportMode.updating.value:
        return query.where(
            repositories.c.import_state.in_(
                [
                    RepositoryImportState.UPDATE_READY,
                    RepositoryImportState.UPDATE_PENDING,
                    RepositoryImportState.UPDATE_LARGE_READY,
                    RepositoryImportState.UPDATE_TIMED_OUT,
                    RepositoryImportState.UPDATE_FAILED,
                ]
            )
        )
    elif import_mode == RepositoryImportMode.polling.value:
        return query.where(
            repositories.c.import_state.in_(
                [
                    RepositoryImportState.CHECK_FOR_UPDATES,
                    RepositoryImportState.SYNC_FAILED,
                ]
            )
        )
    elif import_mode == RepositoryImportMode.disabled.value:
        return query.where(
            repositories.c.import_state == RepositoryImportState.IMPORT_DISABLED
        )
    else:
        assert False, 'Unhandled import mode'
