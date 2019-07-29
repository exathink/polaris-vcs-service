# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam
from polaris.repos.db.model import repositories
from polaris.repos.db.schema import RepositoryImportState
from polaris.graphql.interfaces import NamedNode
from ..interfaces import RepositoryInfo
from ..repository.sql_expressions import repository_info_columns


class ConnectorRepositoriesNodes:
    interfaces = (NamedNode, RepositoryInfo)

    @staticmethod
    def selectable(**kwargs):
        query = select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name,
            *repository_info_columns(repositories)
        ]).where(
            repositories.c.connector_key == bindparam('key')
        )

        if 'unimportedOnly' in kwargs and kwargs['unimportedOnly']:
            query  = query.where(
                repositories.c.import_state == RepositoryImportState.IMPORT_DISABLED
            )

        return query

