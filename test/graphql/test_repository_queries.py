# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



from test.shared_fixtures import *
from graphene.test import Client
from polaris.vcs.service.graphql import schema

class TestRepositoryQueries:

    def it_returns_info_about_a_repository(self, setup_sync_repos):
        client = Client(schema)

        response = client.execute("""
            query getRepositoryInfo($repositoryKey: String!) {
                repository(key: $repositoryKey) {
                    id
                    name
                    key
                    url
                    description
                    importState
                    integrationType
                }
            }
        """, variable_values=dict(
            repositoryKey=test_repository_key
        ))

        assert response['data']
        assert response['data']['repository']
        assert response['data']['repository']['id']
        assert response['data']['repository']['name']
        assert response['data']['repository']['key']
        assert response['data']['repository']['url']
        assert response['data']['repository']['importState']
        assert response['data']['repository']['integrationType']
        assert response['data']['repository']['description']



