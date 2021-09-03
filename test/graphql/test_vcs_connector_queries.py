# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.shared_fixtures import *
from graphene.test import Client
from polaris.vcs.service.graphql import schema
from polaris.repos.db.schema import RepositoryImportState
from polaris.common.enums import VcsIntegrationTypes

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
                                importState
                            }
                        }
                    }
                }
            }
        """, variable_values=dict(
            connectorKey=github_connector_key
        ))

        assert response['data']
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 1


    def it_filters_repositories_by_key(self, setup_sync_repos):
        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!, $keys: [String]!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories(keys: $keys) {
                        edges {
                            node {
                                id
                                name
                                key
                                importState
                            }
                        }
                    }
                }
            }
        """, variable_values=dict(
            connectorKey=github_connector_key,
            keys=[test_repository_key]
        ))

        assert response['data']
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 1

    def it_filters_repositories_by_keys_that_dont_exist(self, setup_sync_repos):
        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!, $keys: [String]!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories(keys: $keys) {
                        edges {
                            node {
                                id
                                name
                                key
                                importState
                            }
                        }
                    }
                }
            }
        """, variable_values=dict(
            connectorKey=github_connector_key,
            keys=[str(uuid.uuid4()), str(uuid.uuid4())]
        ))

        assert response['data']
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 0

@pytest.yield_fixture
def setup_import_state_tests(setup_org_repo, setup_connectors):
    repository, organization = setup_org_repo
    connectors = setup_connectors

    with db.orm_session() as session:
        session.add(organization)
        session.add(repository)
        repository.import_state = RepositoryImportState.IMPORT_DISABLED

        for state in [
            RepositoryImportState.IMPORT_READY,
            RepositoryImportState.IMPORT_PENDING,
            RepositoryImportState.IMPORT_FAILED,
            RepositoryImportState.IMPORT_TIMED_OUT,
            RepositoryImportState.IMPORT_SMALL_READY
        ]:
            organization.repositories.append(
                Repository(
                    connector_key=github_connector_key,
                    organization_key=test_organization_key,
                    key=uuid.uuid4(),
                    name=test_repository_name,
                    source_id=str(uuid.uuid4()),
                    import_state=state,
                    description='A neat new repo',
                    integration_type=VcsIntegrationTypes.github.value,
                    url='https://foo.bar.com'
                )
            )

    yield organization, connectors

class TestRepositoryFilters:

    def it_filters_repositories_by_disabled(self, setup_import_state_tests):
        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories (importMode: disabled){
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
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 1

    def it_filters_repositories_by_importing_state(self, setup_import_state_tests):
        organization, _ = setup_import_state_tests

        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories (importMode: importing){
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
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 5

    def it_filters_repositories_by_updating_state(self, setup_import_state_tests):
        organization, _ = setup_import_state_tests

        with db.orm_session() as session:
            session.add(organization)
            for state in [
                RepositoryImportState.UPDATE_READY,
                RepositoryImportState.UPDATE_PENDING,
                RepositoryImportState.UPDATE_FAILED,
                RepositoryImportState.UPDATE_TIMED_OUT,
                RepositoryImportState.UPDATE_LARGE_READY
            ]:
                organization.repositories.append(
                    Repository(
                        connector_key=github_connector_key,
                        organization_key=test_organization_key,
                        key=uuid.uuid4(),
                        name=test_repository_name,
                        source_id=str(uuid.uuid4()),
                        import_state=state,
                        description='A neat new repo',
                        integration_type=VcsIntegrationTypes.github.value,
                        url='https://foo.bar.com'
                    )
                )

        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories (importMode: updating){
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
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 5

    def it_filters_repositories_by_polling_state(self, setup_import_state_tests):
        organization, _ = setup_import_state_tests

        with db.orm_session() as session:
            session.add(organization)
            for state in [
                RepositoryImportState.CHECK_FOR_UPDATES,
                RepositoryImportState.SYNC_FAILED,
            ]:
                organization.repositories.append(
                    Repository(
                        connector_key=github_connector_key,
                        organization_key=test_organization_key,
                        key=uuid.uuid4(),
                        name=test_repository_name,
                        source_id=str(uuid.uuid4()),
                        import_state=state,
                        description='A neat new repo',
                        integration_type=VcsIntegrationTypes.github.value,
                        url='https://foo.bar.com'
                    )
                )

        client = Client(schema)

        response = client.execute("""
            query getConnectorInfo($connectorKey: String!){
                vcsConnector(key: $connectorKey) {
                    id
                    repositories (importMode: polling){
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
        repositories = response['data']['vcsConnector']['repositories']['edges']
        assert len(repositories) == 2
