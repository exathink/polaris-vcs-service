# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.vcs.messaging.messages import RefreshConnectorRepositories
from polaris.messaging.utils import publish
from polaris.messaging.topics import ConnectorsTopic


def refresh_connector_repositories(connector_key, tracking_receipt=None,  channel=None):
    message = RefreshConnectorRepositories(
        send=dict(
            connector_key=connector_key,
            tracking_receipt_key=tracking_receipt.key if tracking_receipt else None
        )
    )
    publish(
        ConnectorsTopic,
        message,
        channel=channel
    )
    return message
