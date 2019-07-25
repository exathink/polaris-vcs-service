# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from ..interfaces import RepositoryInfo
from ..interface_mixins import RepositoryInfoResolverMixin
from .selectable import RepositoryNode


class Repository(
    RepositoryInfoResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, RepositoryInfo)
        interface_resolvers = {
        }
        named_node_resolver = RepositoryNode
        connection_class = lambda: Repositories

    @classmethod
    def ConnectionField(cls, named_node_resolver=None, **kwargs):
        return super().ConnectionField(
            named_node_resolver,

            projectKeys=graphene.Argument(
                graphene.List(graphene.String), required=False,
                description='Only fetch repositories for the specified projects'
            ),
            **kwargs
        )

    @classmethod
    def resolve_field(cls, info, key, **kwargs):
        return cls.resolve_instance(key, **kwargs)


class Repositories(
    CountableConnection
):
    class Meta:
        node = Repository


class RepositoriesConnectionMixin(ConnectionResolverMixin):
    repositories = Repository.ConnectionField()

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            self.get_connection_resolver_context('repositories'),
            self.get_connection_node_resolver('repositories'),
            self.get_instance_query_params(),
            **kwargs
        )
