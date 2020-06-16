# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.common import db
import polaris.vcs.db.impl.repositories
from polaris.vcs import connector_factory
from polaris.integrations.db.api import tracking_receipt_updates
from polaris.vcs.db import api
from polaris.vcs.messaging import publish
from polaris.utils.exceptions import ProcessingException

log = logging.getLogger('polaris.vcs.service.commands')


def sync_repositories(connector_key, tracking_receipt_key=None):
    connector = connector_factory.get_connector(connector_key=connector_key)
    if connector:
        with tracking_receipt_updates(
                tracking_receipt_key,
                start_info=f"Started refreshing repositories for {connector.name}",
                success_info=f"Finished refreshing repositories for {connector.name}",
                error_info=f"Error refreshing repositories for {connector.name}"
        ):
            for source_repositories in connector.fetch_repositories_from_source():
                yield polaris.vcs.db.api.sync_repositories(
                    connector.organization_key,
                    connector.key,
                    source_repositories
                )


def sync_pull_requests(repository_key, connector_key, source_repo_id):
    log.info(f'Sync pull requests starting')
    connector = connector_factory.get_connector(connector_key=connector_key)
    if connector:
        yield polaris.vcs.db.api.sync_pull_requests(
            connector.organization_key,
            repository_key,
            connector.fetch_pull_requests_from_source(source_repo_id)
        )


def register_repository_push_webhooks(organization_key, connector_key, repository_summaries):
    connector = connector_factory.get_connector(connector_key=connector_key)
    if connector and getattr(connector, 'register_repository_push_hook', None):
        for repo in repository_summaries:
            try:
                webhook_info = connector.register_repository_push_hook(repo)
                api.register_webhook(organization_key, repo['key'], webhook_info)
            except ProcessingException as e:
                log.error(e)


def import_repositories(organization_key, connector_key, repository_keys):
    with db.orm_session():
        result = api.import_repositories(organization_key, repository_keys)
        if result['success']:
            imported_repositories = result['repositories']
            register_repository_push_webhooks(organization_key, connector_key, imported_repositories)
            publish.repositories_imported(organization_key, imported_repositories)
            return result['repositories']
        else:
            log.error(f"Import repositories failed: {result.get('exception')}")
            raise ProcessingException(f"Import repositories failed: {result.get('exception')}")


def handle_remote_repository_push(connector_key, repository_source_id):
    return api.handle_remote_repository_push(connector_key, repository_source_id)


def test_vcs_connector(connector_key, join_this=None):
    with db.orm_session(join_this) as session:
        vcs_connector = connector_factory.get_connector(
            connector_key=connector_key,
            join_this=session
        )
        return vcs_connector.test()
