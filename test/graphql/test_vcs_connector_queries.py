# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.shared_fixtures import *
from graphene.test import Client
from polaris.vcs.service.graphql import schema


class TestVcsConnector:

    def it_returns_info_about_a_vcs_connector(self, setup_sync_repos):
        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    name
                    key
                    connectorType
                    state
                }
            }
        """, variable_values=dict(
            connectorKey=github_connector_key
        ))
        assert response['data']
        assert response['data']['vcsConnector']['id']
        assert response['data']['vcsConnector']['name']
        assert response['data']['vcsConnector']['key']
        assert response['data']['vcsConnector']['connectorType']
        assert response['data']['vcsConnector']['state']

    def it_returns_repositories_for_a_connector(self, setup_sync_repos):
        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories {
                        edges {
                            node {
                                id
                                name
                                key
                            }
                        }
                    }
                }
            }
        """, variable_values=dict(
            connectorKey=github_connector_key
        ))

        assert response['data']
        repositories = response['data']['vcsConnector']['repositories']
        assert len(repositories) == 1
