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
from polaris.common.enums import ConnectorType

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

def sync_repository_forks(connector_key, repository_key, join_this=None):
    with db.orm_session(join_this) as session:
        connector = connector_factory.get_connector(
            connector_key=connector_key,
            join_this=session
        )
        if connector:
            repository = repository_factory.get_provider_impl(repository_key, join_this=session)
            if repository is not None:
                if hasattr(repository, 'fetch_repository_forks'):
                    for source_repositories in repository.fetch_repository_forks():
                        yield polaris.vcs.db.api.sync_repositories(
                            connector.organization_key,
                            connector.key,
                            source_repositories
                        )
                else:
                    raise ProcessingException(
                        'The fetch repository forks operation is not implemented for this connector')
            else:
                raise ProcessingException(f'Repository with key {repository_key} was not found')


def sync_pull_requests(repository_key, pull_request_key=None):
    log.info(f'Sync pull requests starting')
    repository_provider = repository_factory.get_provider_impl(repository_key)
    if repository_provider:

        if pull_request_key is not None:
            pull_request = api.find_pull_request(pull_request_key)
            if pull_request is not None:
                yield api.sync_pull_requests(
                    repository_key,
                    repository_provider.fetch_pull_requests_from_source(pull_request_source_id=pull_request.api_id)
                )
            else:
                raise ProcessingException(f'Could not find pull request with key {pull_request_key}')
        else:
            yield api.sync_pull_requests(
                repository_key,
                repository_provider.fetch_pull_requests_from_source()
            )
    else:
        return []


def register_repository_webhooks(connector_key, repository_key, join_this=None):
    with db.orm_session(join_this) as session:
        try:
            connector = connector_factory.get_connector(connector_key=connector_key, join_this=session)
            if connector and getattr(connector, 'register_repository_webhooks', None):
                repo = Repository.find_by_repository_key(session, repository_key=repository_key)
                if repo:
                    get_hooks_result = api.get_registered_webhooks(repository_key, join_this=session)
                    if get_hooks_result['success']:
                        webhook_info = connector.register_repository_webhooks(repo.source_id,
                                                                              get_hooks_result['registered_webhooks'])
                        if webhook_info['success']:
                            register_result = api.register_webhooks(repository_key, webhook_info, join_this=session)
                            if register_result['success']:
                                return dict(
                                    success=True,
                                    repository_key=repository_key
                                )
                            else:
                                return db.failure_message(f"Could not register webhook due to: {register_result.get('exception')}")
                else:
                    return db.failure_message(f"Could not find repository with key {repository_key}")
            elif connector:
                # TODO: Remove this when github and bitbucket register webhook implementation is done.
                return dict(
                    success=True,
                    repository_key=repository_key
                )
            else:
                return db.failure_message(f"Could not find connector with key {connector_key}")
        except ProcessingException as e:
            return db.failure_message(f"Register webhooks failed due to: {e}")


def register_repositories_webhooks(connector_key, repository_keys, join_this=None):
    result = []
    for repository_key in repository_keys:
        registration_status = register_repository_webhooks(connector_key, repository_key, join_this=join_this)
        if registration_status['success']:
            result.append(registration_status)
        else:
            result.append(dict(
                repository_key=repository_key,
                success=False,
                message=registration_status.get('message'),
                exception=registration_status.get('exception')
            ))
    return result


def import_repositories(organization_key, connector_key, repository_keys):
    with db.orm_session() as session:
        result = api.import_repositories(organization_key, repository_keys)
        if result['success']:
            imported_repositories = result['repositories']
            for repo in imported_repositories:
                register_webhooks_result = register_repository_webhooks(connector_key, repo['key'], join_this=session)
                if not register_webhooks_result['success']:
                    # we dont raise an error when register_web_hook repositories fails.
                    # worst case we should fall back on polling for these, and
                    # we can always go back try re-registering webhooks for them using the mutation. 
                    log.error(
                        f"Import repositories failed: {register_webhooks_result.get('exception')}"
                    )
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


def get_pull_request_summary(pull_request_key, join_this=None):
    return api.get_pull_request_summary(pull_request_key, join_this)


