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
from polaris.work_tracking import publish

log = logging.getLogger('polaris.vcs.bitbucket_connector')

config_provider = get_config_provider()


class BitBucketConnectorContext:
    base_url = config_provider.get('BITBUCKET_CONNECTOR_BASE_URL')
    mount_path = config_provider.get('MOUNT_PATH')

    app_name = "Urjuna BitBucket Connector"
    addon_name = "Urjuna BitBucket Connector"
    addon_key = config_provider.get('BITBUCKET_CONNECTOR_APP_KEY', 'polaris.bitbucket')
    addon_description = "BitBucket Connector for Urjuna"
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






