# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.integrations.atlassian_connect import PolarisAtlassianConnect
from polaris.integrations.db.api import load_atlassian_connect_record, enable_atlassian_connect_record
from polaris.utils.config import get_config_provider
from polaris.vcs.messaging import publish
from polaris.utils.exceptions import ConfigurationException

log = logging.getLogger('polaris.vcs.bitbucket_connector')

config_provider = get_config_provider()


class BitBucketConnectorContext:
    base_url = config_provider.get('BITBUCKET_CONNECTOR_BASE_URL')
    mount_path = config_provider.get('MOUNT_PATH')

    app_name = "Urjuna Connector for BitBucket"
    addon_name = "Urjuna Connector for BitBucket"
    addon_key = config_provider.get('BITBUCKET_CONNECTOR_APP_KEY')
    if addon_key is None:
        raise ConfigurationException("BITBUCKET_CONNECTOR_APP_KEY must be specified")

    addon_description = "Urjuna Connector for BitBucket"
    addon_scopes = ['repository', 'issue', 'pullrequest', 'webhook']
    addon_version = 1


def init_connector(app):
    log.info("Initializing Atlassian Connector for BitBucket")

    ac = PolarisAtlassianConnect(app, connector_context=BitBucketConnectorContext)

    @ac.lifecycle("installed")
    def lifecycle_installed(client):
        log.info(f'Connector installed: {client.baseUrl} ({client.clientKey})')
        # Bitbucket connector does not call the enabled lifecycle unlike the jira connector.
        # Since we expect the connector to be enabled after install in the rest of the app
        # we do it manually here.
        enable_atlassian_connect_record(client.clientKey)
        # call the lifecyle method explicitly to keep the rest of the app protocol the same,
        lifecycle_enabled(client)

    @ac.lifecycle("uninstalled")
    def lifecycle_uninstalled(client):
        log.info(f'Connector uninstalled: {client.baseUrl} ({client.clientKey})')

    @ac.lifecycle("enabled")
    def lifecycle_enabled(client):
        log.info(f'Connector enabled: {client.baseUrl} ({client.clientKey})')
        connector_record = load_atlassian_connect_record(client.clientKey)
        if connector_record:
            publish.connector_event(
                connector_key=connector_record.key,
                connector_type=connector_record.type,
                product_type=connector_record.product_type,
                event='enabled'
            )

    @ac.lifecycle("disabled")
    def lifecycle_disabled(client):
        log.info(f'Connector disabled: {client.baseUrl} ({client.clientKey})')

    @ac.webhook("repo:created")
    def handle_repo_created(client, event):
        log.info(f' repo:created Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:created',
            atlassian_event=event
        )

    @ac.webhook("repo:updated")
    def handle_repo_updated(client, event):
        log.info(f' repo:updated Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:updated',
            atlassian_event=event
        )

    @ac.webhook("repo:deleted")
    def handle_repo_deleted(client, event):
        log.info(f' repo:deleted Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:deleted',
            atlassian_event=event
        )

    @ac.webhook("repo:push")
    def handle_repo_push(client, event):
        log.info(f' repo:push Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:push',
            atlassian_event=event
        )

    @ac.webhook("repo:fork")
    def handle_repo_fork(client, event):
        log.info(f' repo:fork Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:fork',
            atlassian_event=event
        )

    @ac.webhook("repo:branch_created")
    def handle_repo_branch_created(client, event):
        log.info(f' repo:branch_created Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:branch_created',
            atlassian_event=event
        )

    @ac.webhook("repo:branch_deleted")
    def handle_repo_branch_deleted(client, event):
        log.info(f' repo:branch_deleted Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:branch_deleted',
            atlassian_event=event
        )

    @ac.webhook("repo:commit_comment_created")
    def handle_repo_commit_comment_created(client, event):
        log.info(f' repo:commit_comment_created Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:commit_comment_created',
            atlassian_event=event
        )

    @ac.webhook("repo:commit_status_created")
    def handle_repo_commit_status_created(client, event):
        log.info(f' repo:commit_status_created Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:commit_status_created',
            atlassian_event=event
        )

    @ac.webhook("repo:commit_status_updated")
    def handle_repo_commit_status_updated(client, event):
        log.info(f' repo:commit_status_updated Event Received: {client.baseUrl} ({client.clientKey})')
        publish.atlassian_connect_repository_event(
            atlassian_connector_key=client.atlassianConnectorKey,
            atlassian_event_type='repo:commit_status_updated',
            atlassian_event=event
        )

