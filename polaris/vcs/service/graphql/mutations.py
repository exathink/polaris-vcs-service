# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging

from polaris.common import db
from polaris.integrations.db.api import create_tracking_receipt
from polaris.vcs.messaging import publish
from polaris.vcs import commands

logger = logging.getLogger('polaris.vcs.graphql.mutations')


class RefreshConnectorRepositoriesInput(graphene.InputObjectType):
    connector_key = graphene.String(required=True)
    track = graphene.Boolean(required=False, default_value=False)


class RefreshConnectorRepositories(graphene.Mutation):
    class Arguments:
        refresh_connector_repositories_input = RefreshConnectorRepositoriesInput(required=True)

    success = graphene.Boolean()
    tracking_receipt_key = graphene.String(required=False)

    def mutate(self, info, refresh_connector_repositories_input):
        with db.orm_session() as session:
            tracking_receipt = None
            if refresh_connector_repositories_input.track:
                tracking_receipt = create_tracking_receipt(
                    name='RefreshConnectorRepositoriesMutation',
                    join_this=session
                )

            publish.refresh_connector_repositories(refresh_connector_repositories_input['connector_key'],
                                                   tracking_receipt)

            return RefreshConnectorRepositories(
                success=True,
                tracking_receipt_key=tracking_receipt.key if tracking_receipt else None
            )


class ImportRepositoriesInput(graphene.InputObjectType):
    organization_key = graphene.String(required=True)
    repository_keys = graphene.List(graphene.String, required=True)


class ImportRepositories(graphene.Mutation):
    class Arguments:
        import_repositories_input = ImportRepositoriesInput(required=True)

    success = graphene.Boolean()
    imported_repository_keys = graphene.List(graphene.String)

    def mutate(self, info, import_repositories_input):
        organization_key = import_repositories_input['organization_key']
        logger.info(f"Processing Import Repositories for organization {organization_key}")

        imported_repositories = commands.import_repositories(
            organization_key,
            import_repositories_input['repository_keys']
        )
        return ImportRepositories(
            success=True,
            imported_repository_keys=[
                repository['key']
                for repository in imported_repositories
            ]
        )
