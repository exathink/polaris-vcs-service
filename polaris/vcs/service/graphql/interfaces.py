# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene


class RepositoryInfo(graphene.Interface):
    url = graphene.String()
    description = graphene.String()
    integration_type = graphene.String()
    public = graphene.Boolean()
    import_state = graphene.String()
    commit_count = graphene.Int()


class SyncStateSummary(graphene.Interface):
    commits_in_process = graphene.Int()
