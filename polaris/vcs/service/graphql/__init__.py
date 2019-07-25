# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from polaris.integrations.graphql import IntegrationsQueryMixin, IntegrationsMutationsMixin
from polaris.graphql.interfaces import NamedNode

from .mutations import RefreshConnectorRepositories
from .repository import Repository
from .vcs_connector import VcsConnector


class Query(
    IntegrationsQueryMixin,
    graphene.ObjectType
):
    node = NamedNode.Field()

    vcs_connector = VcsConnector.Field()
    repository = Repository.Field()

    def resolve_repository(self, info, **kwargs):
        return Repository.resolve_field(info, **kwargs)

    def resolve_vcs_connector(self, info, **kwargs):
        return VcsConnector.resolve_field(info, **kwargs)


class Mutation(
    IntegrationsMutationsMixin,
    graphene.ObjectType
):
    refresh_connector_repositories = RefreshConnectorRepositories.Field()


schema = graphene.Schema(query=Query, mutation=Mutation)
