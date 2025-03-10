# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.messaging.messages import RepositoriesImported, PullRequestsUpdated, PullRequestsCreated
from polaris.vcs.messaging.messages import RefreshConnectorRepositories, AtlassianConnectRepositoryEvent, \
    GitlabRepositoryEvent, RemoteRepositoryPushEvent, GithubRepositoryEvent, AzureRepositoryEvent, SyncPullRequest
from polaris.messaging.utils import publish
from polaris.messaging.topics import ConnectorsTopic, VcsTopic
from polaris.integrations.publish import connector_event


def refresh_connector_repositories(connector_key, tracking_receipt=None, channel=None):
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


def repositories_imported(organization_key, imported_repositories, channel=None):
    message = RepositoriesImported(
        send=dict(
            organization_key=organization_key,
            imported_repositories=imported_repositories
        )
    )
    publish(VcsTopic, message, channel=channel)
    return message


def atlassian_connect_repository_event(atlassian_connector_key, atlassian_event_type, atlassian_event, channel=None):
    message = AtlassianConnectRepositoryEvent(
        send=dict(
            atlassian_connector_key=atlassian_connector_key,
            atlassian_event_type=atlassian_event_type,
            atlassian_event=atlassian_event
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )
    return message


def gitlab_repository_event(event_type, connector_key, payload, channel=None):
    message = GitlabRepositoryEvent(
        send=dict(
            event_type=event_type,
            connector_key=connector_key,
            payload=payload
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )
    return message


def github_repository_event(event_type, connector_key, payload, channel=None):
    message = GithubRepositoryEvent(
        send=dict(
            event_type=event_type,
            connector_key=connector_key,
            payload=payload
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )
    return message

def azure_repository_event(event_type, connector_key, payload, channel=None):
    message = AzureRepositoryEvent(
        send=dict(
            event_type=event_type,
            connector_key=connector_key,
            payload=payload
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )
    return message


def remote_repository_push_event(connector_key, repository_source_id, channel=None):
    message = RemoteRepositoryPushEvent(
        send=dict(
            connector_key=connector_key,
            repository_source_id=repository_source_id
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )
    return message


def pull_request_created_event(organization_key, repository_key, pull_request_summaries, channel=None):
    message = PullRequestsCreated(
        send=dict(
            organization_key=organization_key,
            repository_key=repository_key,
            pull_request_summaries=pull_request_summaries
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )


def pull_request_updated_event(organization_key, repository_key, pull_request_summaries, channel=None):
    message = PullRequestsUpdated(
        send=dict(
            organization_key=organization_key,
            repository_key=repository_key,
            pull_request_summaries=pull_request_summaries
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )


def sync_pull_request(organization_key, repository_key, pull_request_key=None, pull_request_source_id=None, channel=None):
    message = SyncPullRequest(
        send=dict(
            organization_key=organization_key,
            repository_key=repository_key,
            pull_request_key=pull_request_key,
            pull_request_source_id=pull_request_source_id
        )
    )
    publish(
        VcsTopic,
        message,
        channel=channel
    )




# This shim is here only to explictly mark connector event as a referenced symbol.
# PyCharm apparently does not correctly recognize re-exported names. In this case publish.connector_events
# is marked as an unreferenced name and optimized out if we do optimize imports. This causes run time failures
# when this function is called. Hacky fix is to simply assign this imported name to _dont_optimize_import
# so the import is
# not optimized out by mistake.
_dont_optimize_import = (connector_event,)
