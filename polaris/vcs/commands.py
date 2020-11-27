# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.common import db
import polaris.vcs.db.impl.repositories
from polaris.vcs import connector_factory, repository_factory
from polaris.integrations.db.api import tracking_receipt_updates
from polaris.vcs.db import api
from polaris.vcs.messaging import publish
from polaris.utils.exceptions import ProcessingException
from polaris.repos.db.model import Repository

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


def sync_pull_requests(repository_key):
    log.info(f'Sync pull requests starting')
    repository_provider = repository_factory.get_provider_impl(repository_key)
    if repository_provider:
        yield polaris.vcs.db.api.sync_pull_requests(
            repository_key,
            repository_provider.fetch_pull_requests_from_source()
        )
    else:
        return []


def register_repository_webhooks(connector_key, repository_key, join_this=None):
    with db.orm_session(join_this) as session:
        connector = connector_factory.get_connector(connector_key=connector_key, join_this=session)
        if connector and getattr(connector, 'register_repository_webhooks', None):
            repo = Repository.find_by_repository_key(session, repository_key=repository_key)
            if repo:
                try:
                    registered_webhooks = api.get_registered_webhooks(repository_key, join_this=session)
                    webhook_info = connector.register_repository_webhooks(repo.source_id, registered_webhooks)
                    api.register_webhooks(repository_key, webhook_info, join_this=session)
                    return True
                except ProcessingException as e:
                    log.error(e)
                    return ProcessingException(f"Register webhooks failed for repository with id {repo.id} due to: {e}")
            else:
                return ProcessingException(f"Could not find repository with key {repository_key}")
        else:
            return ProcessingException(f"Could not find connector with key {connector_key}")


def register_repositories_webhooks(connector_key, repository_keys, join_this=None):
    result = []
    for repository_key in repository_keys:
        registration_status = register_repository_webhooks(connector_key, repository_key, join_this=join_this)
        if registration_status == True:
            result.append({'repository_key': repository_key, 'status': registration_status, 'error_message': None})
        else:
            result.append({'repository_key': repository_key, 'status': False, 'error_message': registration_status})
    return result


def import_repositories(organization_key, connector_key, repository_keys):
    with db.orm_session() as session:
        result = api.import_repositories(organization_key, repository_keys)
        if result['success']:
            imported_repositories = result['repositories']
            register_repository_webhooks(connector_key, imported_repositories, join_this=session)
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
        if vcs_connector:
            return vcs_connector.test()
