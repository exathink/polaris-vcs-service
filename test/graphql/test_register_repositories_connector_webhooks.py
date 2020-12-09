# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
import pytest
from unittest.mock import patch
from polaris.utils.collections import Fixture
from test.shared_fixtures import *
from graphene.test import Client
from polaris.vcs.service.graphql import schema
from polaris.common import db


class TestRegisterRepositoriesConnectorWebhooks:

    class TestWithGitlabConnector:

        @pytest.yield_fixture()
        def setup(self, setup_sync_repos_gitlab):
            organization_key, connectors = setup_sync_repos_gitlab
            repository_key = test_repository_key
            registered_events = ['push_events', 'merge_requests_events']
            gitlab_connector_key = connectors['gitlab']
            active_hook_id = '1000'
            mutation_string = """
                mutation registerRepositoriesConnectorWebhooks($registerWebhooksInput: RegisterWebhooksInput!) {
                    registerRepositoriesConnectorWebhooks(registerWebhooksInput: $registerWebhooksInput){
                    webhooksRegistrationStatus {
                      repositoryKey
                      success
                      errorMessage
                    }
                  }
                }
            """
            yield Fixture(
                connector_key=gitlab_connector_key,
                repository_key=repository_key,
                registered_events=registered_events,
                mutation_string=mutation_string,
                active_hook_id=active_hook_id
            )

        def it_registers_new_webhook_and_updates_info(self, setup):
            fixture = setup
            client = Client(schema)

            with patch('polaris.vcs.integrations.gitlab.GitlabRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                        active_webhook=fixture.active_hook_id,
                        deleted_webhooks=[],
                        registered_events=fixture.registered_events
                    )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=str(fixture.connector_key),
                            repositoryKeys=[str(fixture.repository_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert status[0]['success']

                assert db.connection().execute(
                    f"select count(*) from repos.repositories \
                    where key='{fixture.repository_key}' \
                    and polling=false \
                    and source_data->>'active_webhook'='{fixture.active_hook_id}'"\
                ).scalar()==1

        def it_re_registers_webhooks_and_updates_source_data(self, setup):
            fixture = setup
            client = Client(schema)
            new_webhook_id = 1001

            with patch(
                    'polaris.vcs.integrations.gitlab.GitlabRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                    active_webhook=fixture.active_hook_id,
                    deleted_webhooks=[fixture.active_hook_id],
                    registered_events=fixture.registered_events
                )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=str(fixture.connector_key),
                            repositoryKeys=[str(fixture.repository_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert status[0]['success']

                assert db.connection().execute(
                    f"select count(*) from repos.repositories \
                                where key='{fixture.repository_key}' \
                                and polling=false \
                                and source_data->>'active_webhook'='{fixture.active_hook_id}'" \
                    ).scalar() == 1

            with patch(
                    'polaris.vcs.integrations.gitlab.GitlabRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                    active_webhook=new_webhook_id,
                    deleted_webhooks=[fixture.active_hook_id],
                    registered_events=fixture.registered_events
                )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=str(fixture.connector_key),
                            repositoryKeys=[str(fixture.repository_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert status[0]['success']

                assert db.connection().execute(
                    f"select count(*) from repos.repositories \
                                where key='{fixture.repository_key}' \
                                and polling=false \
                                and source_data->>'active_webhook'='{new_webhook_id}' \
                                and source_data->>'inactive_webhooks'='[]'" \
                    ).scalar() == 1

        def it_returns_connector_not_found_when_connector_id_is_incorrect(self, setup):
            fixture = setup
            client = Client(schema)
            test_connector_key = str(uuid.uuid4())
            with patch(
                    'polaris.vcs.integrations.gitlab.GitlabRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                    active_webhook=fixture.active_hook_id,
                    deleted_webhooks=[],
                    registered_events=fixture.registered_events
                )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=test_connector_key,
                            repositoryKeys=[str(fixture.repository_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert not status[0]['success']
                assert status[0]['errorMessage'] == f"Register webhooks failed due to: Cannot find connector for connector_key {test_connector_key}"

        def it_returns_repository_not_found_error_when_repository_id_is_incorrect(self, setup):
            fixture = setup
            client = Client(schema)
            test_repo_key = str(uuid.uuid4())
            with patch(
                    'polaris.vcs.integrations.gitlab.GitlabRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                    active_webhook=fixture.active_hook_id,
                    deleted_webhooks=[],
                    registered_events=fixture.registered_events
                )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=str(fixture.connector_key),
                            repositoryKeys=[str(test_repo_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert not status[0]['success']
                assert status[0]['errorMessage'] == f"Could not find repository with key {test_repo_key}"


    class TestWithGithubConnector:
        @pytest.yield_fixture()
        def setup(self, setup_sync_repos):
            organization_key, connectors = setup_sync_repos
            repository_key = test_repository_key
            registered_events = ['push', 'pull_request']
            github_connector_key = connectors['github']
            active_hook_id = '1000'
            mutation_string = """
                        mutation registerRepositoriesConnectorWebhooks($registerWebhooksInput: RegisterWebhooksInput!) {
                            registerRepositoriesConnectorWebhooks(registerWebhooksInput: $registerWebhooksInput){
                            webhooksRegistrationStatus {
                              repositoryKey
                              success
                              errorMessage
                            }
                          }
                        }
                    """
            yield Fixture(
                connector_key=github_connector_key,
                repository_key=repository_key,
                registered_events=registered_events,
                mutation_string=mutation_string,
                active_hook_id=active_hook_id
            )

        def it_registers_new_webhook_and_updates_info(self, setup):
            fixture = setup
            client = Client(schema)

            with patch(
                    'polaris.vcs.integrations.github.GithubRepositoriesConnector.register_repository_webhooks') as register_webhooks:
                register_webhooks.return_value = dict(
                    active_webhook=fixture.active_hook_id,
                    deleted_webhooks=[],
                    registered_events=fixture.registered_events
                )
                response = client.execute(
                    fixture.mutation_string,
                    variable_values=dict(
                        registerWebhooksInput=dict(
                            connectorKey=str(fixture.connector_key),
                            repositoryKeys=[str(fixture.repository_key)]
                        )
                    )
                )
                assert 'data' in response
                status = response['data']['registerRepositoriesConnectorWebhooks']['webhooksRegistrationStatus']
                assert len(status) == 1
                assert status[0]['success']

                assert db.connection().execute(
                    f"select count(*) from repos.repositories \
                                    where key='{fixture.repository_key}' \
                                    and polling=false \
                                    and source_data->>'active_webhook'='{fixture.active_hook_id}'" \
                    ).scalar() == 1