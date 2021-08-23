# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging

from polaris.common import db
from polaris.integrations.db.api import create_tracking_receipt, create_connector, update_connector
from polaris.integrations.graphql import CreateConnector, EditConnector
from polaris.integrations import publish as integrations_publish
from polaris.utils.exceptions import ProcessingException

from polaris.vcs.messaging import publish
from polaris.vcs import commands
from .vcs_connector import VcsConnector

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
    connector_key = graphene.String(required=True)
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
            import_repositories_input['connector_key'],
            import_repositories_input['repository_keys']
        )
        return ImportRepositories(
            success=True,
            imported_repository_keys=[
                repository['key']
                for repository in imported_repositories
            ]
        )


class TestConnectorInput(graphene.InputObjectType):
    connector_key = graphene.String(required=True)


class TestVcsConnector(graphene.Mutation):
    class Arguments:
        test_connector_input = TestConnectorInput(required=True)

    success = graphene.Boolean()

    def mutate(self, info, test_connector_input):
        connector_key = test_connector_input.connector_key
        logger.info(f'Test Connector called for connector {connector_key}')
        with db.orm_session() as session:
            return TestVcsConnector(
                success=commands.test_vcs_connector(connector_key, join_this=session)
            )


class CreateVcsConnector(CreateConnector):
    connector = VcsConnector.Field(key_is_required=False)

    def mutate(self, info, create_connector_input):
        logger.info('Create WorkTracking Connector called')
        with db.orm_session() as session:
            connector = create_connector(create_connector_input.connector_type, create_connector_input,
                                         join_this=session)

            # if the connector is created in a non-enabled state (Atlassian for example)
            # we cannot test it. So default is assume test pass.
            can_create = True
            if connector.state == 'enabled':
                can_create = commands.test_vcs_connector(connector.key, join_this=session)

            if can_create:
                resolved = CreateConnector(
                    connector=VcsConnector.resolve_field(info, connector.key)
                )
                # Do the publish right at the end.
                integrations_publish.connector_created(connector)
                return resolved
            else:
                raise ProcessingException("Could not create connector: Connector test failed")


class EditVcsConnector(EditConnector):
    connector = VcsConnector.Field(key_is_required=False)

    def mutate(self, info, edit_connector_input):
        logger.info('Update VCS Connector called')
        with db.orm_session() as session:
            connector = update_connector(edit_connector_input.connector_type, edit_connector_input,
                                         join_this=session)
            if commands.test_vcs_connector(connector.key, join_this=session):
                resolved = EditConnector(
                    connector=VcsConnector.resolve_field(info, connector.key)
                )
                # Do the publish right at the end.
                # integrations_publish.connector_created(connector)
                return resolved
            else:
                raise ProcessingException("Could not update connector: Connector test failed")


class RegisterWebhooksInput(graphene.InputObjectType):
    connector_key = graphene.String(required=True)
    repository_keys = graphene.List(graphene.String, required=True)


class WebhooksRegistrationStatus(graphene.ObjectType):
    repository_key = graphene.String(required=True)
    success = graphene.Boolean(required=True)
    message = graphene.String(required=False)
    exception = graphene.String(required=False)


class RegisterRepositoriesConnectorWebhooks(graphene.Mutation):
    class Arguments:
        register_webhooks_input = RegisterWebhooksInput(required=True)

    webhooks_registration_status = graphene.List(WebhooksRegistrationStatus)

    def mutate(self, info, register_webhooks_input):
        connector_key = register_webhooks_input.connector_key
        repository_keys = register_webhooks_input.repository_keys

        logger.info(f'Register webhooks called for connector: {connector_key}')
        with db.orm_session() as session:
            result = commands.register_repositories_webhooks(connector_key, repository_keys, join_this=session)
            if result:
                return RegisterRepositoriesConnectorWebhooks(
                    webhooks_registration_status=[
                        WebhooksRegistrationStatus(
                            repository_key=status.get('repository_key'),
                            success=status.get('success'),
                            message=status.get('message'),
                            exception=status.get('exception')
                        )
                        for status in result]
                )


class SyncPullRequestsInput(graphene.InputObjectType):
    organization_key = graphene.String(required=True)
    repository_key = graphene.String(required=True)
    pull_request_key = graphene.String(required=False)


class SyncPullRequests(graphene.Mutation):
    class Arguments:
        sync_pull_requests_input = SyncPullRequestsInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, sync_pull_requests_input):
        publish.sync_pull_requests(
            sync_pull_requests_input.organization_key,
            sync_pull_requests_input.repository_key,
            sync_pull_requests_input.pull_request_key
        )
        return SyncPullRequests(
            success=True
        )
