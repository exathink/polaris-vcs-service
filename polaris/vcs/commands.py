# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
import polaris.vcs.db.impl.repositories
from polaris.vcs import connector_factory
from polaris.integrations.db.api import tracking_receipt_updates
from polaris.vcs.db import api
from polaris.vcs.messaging import publish


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


def register_repository_push_webhooks(organization_key, connector_key, repository_summaries):
    connector = connector_factory.get_connector(connector_key=connector_key)
    if connector and getattr(connector, 'register_repository_push_hook', None):
        for repo in repository_summaries:
            webhook_info = connector.register_repository_push_hook(repo)
            api.register_webhook(organization_key, repo['key'], webhook_info)


def import_repositories(organization_key, connector_key, repository_keys):
    with db.orm_session():
        result = api.import_repositories(organization_key, repository_keys)
        if result['success']:
            imported_repositories = result['repositories']
            register_repository_push_webhooks(organization_key, connector_key, imported_repositories)
            publish.repositories_imported(organization_key, imported_repositories)
            return result['repositories']


def test_vcs_connector(connector_key, join_this=None):
    with db.orm_session(join_this) as session:
        vcs_connector = connector_factory.get_connector(
            connector_key=connector_key,
            join_this=session
        )
        return vcs_connector.test()
