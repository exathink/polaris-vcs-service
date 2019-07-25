# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.common import db
from polaris.integrations.db.api import create_tracking_receipt
from polaris.vcs.messaging import publish

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

            publish.refresh_connector_repositories(refresh_connector_repositories_input['connector_key'], tracking_receipt)

            return RefreshConnectorRepositories(
                success=True,
                tracking_receipt_key=tracking_receipt.key if tracking_receipt else None
            )
