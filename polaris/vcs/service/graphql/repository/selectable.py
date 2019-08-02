# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, bindparam, func

from polaris.graphql.interfaces import NamedNode
from ..interfaces import RepositoryInfo, SyncStateSummary

from polaris.repos.db.model import repositories
from polaris.repos.db.schema import commits

from .sql_expressions import repository_info_columns


class RepositoryNode:
    interfaces = (NamedNode, RepositoryInfo)

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key.label('key'),
            repositories.c.name,
            *repository_info_columns(repositories)

        ]).select_from(
            repositories
        ).where(repositories.c.key == bindparam('key'))


class RepositorySyncStateSummary:
    interface = SyncStateSummary

    @staticmethod
    def selectable(repository_nodes, **kwargs):
        return select(
            [
                repository_nodes.c.id,
                func.count(commits.c.id).label('commits_in_process')
            ]
        ).select_from(
            repository_nodes.outerjoin(
                commits, commits.c.repository_id == repository_nodes.c.id
            )
        ).where(
            commits.c.sync_state == 0
        ).group_by(
            repository_nodes.c.id
        )
