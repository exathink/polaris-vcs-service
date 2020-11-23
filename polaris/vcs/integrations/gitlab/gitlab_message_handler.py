# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json

from polaris.vcs.messaging import publish
from polaris.common import db
from polaris.repos.db.model import Repository
from polaris.vcs import connector_factory
from polaris.vcs.db import api
from polaris.vcs.integrations.gitlab import GitlabRepository


def handle_gitlab_repository_push(connector_key, payload, channel=None):
    event = json.loads(payload)
    repo_source_id = event.get('project_id')

    publish.remote_repository_push_event(connector_key, repo_source_id, channel)


def handle_gitlab_pull_request_event(connector_key, payload, channel=None):
    event = json.loads(payload)
    repo_source_id = str(event.get('project')['id'])
    with db.orm_session() as session:
        source_repo = Repository.find_by_connector_key_source_id(
            session,
            connector_key=connector_key,
            source_id=repo_source_id)
        if source_repo:
            connector = connector_factory.get_connector(
                connector_key=source_repo.connector_key,
                join_this=session
            )
            if connector:
                gitlab_repo = GitlabRepository(source_repo, connector)
                pr_object = event.get('object_attributes')
                pull_request_data = gitlab_repo.map_pull_request_info(pr_object)

                result = api.sync_pull_requests(source_repo.key, [[pull_request_data]])
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


def handle_gitlab_event(connector_key, event_type, payload, channel=None):
    if event_type == 'push':
        return handle_gitlab_repository_push(connector_key, payload, channel)
    if event_type == 'merge_request':
        return handle_gitlab_pull_request_event(connector_key, payload, channel)
