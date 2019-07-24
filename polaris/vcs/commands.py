# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import polaris.vcs.db.impl.repositories
from polaris.vcs import connector_factory
from polaris.integrations.db.api import tracking_receipt_updates
from polaris.vcs.db import api


def sync_repositories(connector_key, tracking_receipt_key=None):
    connector = connector_factory.get_connector(connector_key=connector_key)
    if connector:
        with tracking_receipt_updates(
                tracking_receipt_key,
                start_info=f"Started refreshing repsoitories for {connector.name}",
                success_info=f"Finished refreshing repositories for {connector.name}",
                error_info=f"Error refreshing repositories for {connector.name}"
        ):
            for source_repositories in connector.fetch_repositories_from_source():
                yield polaris.vcs.db.api.sync_repositories(
                    connector.organization_key,
                    connector.key,
                    source_repositories
                )