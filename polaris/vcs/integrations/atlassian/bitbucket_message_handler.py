# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging
import json
from polaris.vcs.messaging import publish
from polaris.common import db
from polaris.vcs.db import api
from polaris.repos.db.model import Repository
from polaris.vcs import connector_factory
from polaris.vcs.integrations.atlassian import BitBucketRepository

logger = logging.getLogger('polaris.vcs.integrations.bitbucket.message_handler')


def handle_atlassian_connect_repository_event(connector_key, event_type, event):
    if event_type == 'repo:push':
        return handle_repo_push(connector_key, event)
    if event_type in ['pullrequest:created',
                      'pullrequest:updated',
                      'pullrequest:approved',
                      'pullrequest:unapproved',
                      'pullrequest:fulfilled',
                      'pullrequest:rejected',
                      'pullrequest:comment_created',
                      'pullrequest:comment_deleted']:
        return handle_pull_request_event(connector_key, event)


def handle_repo_push(connector_key, event):
    payload = json.loads(event)
    repo_source_id = payload['data']['repository']['uuid']
    logger.info(f'Received repo:push event for bitbucket connector {connector_key}')

    publish.remote_repository_push_event(connector_key, repo_source_id)


def handle_pull_request_event(connector_key, event):
    payload = json.loads(event)
    repo_source_id = payload['data']['repository']['uuid']
    with db.orm_session() as session:
        source_repo = Repository.find_by_connector_key_source_id(
            session,
            connector_key=connector_key,
            source_id=repo_source_id
        )
        if source_repo:
            connector = connector_factory.get_connector(
                connector_key=source_repo.connector_key,
                join_this=session
            )
            if connector:
                bitbucket_repo = BitBucketRepository(source_repo, connector)
                pr_data = payload['data']['pullrequest']
                mapped_pr_data = bitbucket_repo.map_pull_request_info(pr_data)

                result = api.sync_pull_requests(source_repo.key, [[mapped_pr_data]])
                if result['success']:
                    synced_prs = result['pull_requests']
                    if len(synced_prs) > 0:
                        if synced_prs[0]['is_new']:
                            publish.pull_request_created_event(
                                organization_key=source_repo.organization_key,
                                repository_key=source_repo.key,
                                pull_request_summaries=synced_prs
                            )
                        else:
                            publish.pull_request_updated_event(
                                organization_key=source_repo.organization_key,
                                repository_key=source_repo.key,
                                pull_request_summaries=synced_prs
                            )
                    return synced_prs



