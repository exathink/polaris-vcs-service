# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene


class Query(graphene.ObjectType):
    ping = graphene.String()

    def resolve_ping(self, info):
        return 'ok'


schema = graphene.Schema(query=Query)
