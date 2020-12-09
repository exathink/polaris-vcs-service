# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json
from datetime import datetime
from polaris.vcs.messaging import publish
from polaris.common import db
from polaris.repos.db.model import Repository
from polaris.vcs import connector_factory
from polaris.vcs.db import api
from polaris.vcs.integrations.github import GithubRepository
from polaris.utils.collections import DictToObj


def handle_github_repository_push(connector_key, payload, channel=None):
    pass


def handle_github_pull_request_event(connector_key, payload, channel=None):
    event = json.loads(payload)
    repo_source_id = str(event.get('repository')['id'])
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
                github_repo = GithubRepository(source_repo, connector)
                pr_dict = event.get('pull_request')

                # Convert pr_dict to object including nested objects
                pr_object = DictToObj(pr_dict)
                # FIXME: Hack: Convert all datetime strings to datetime objects before mapping
                pr_object.created_at = datetime.strptime(pr_object.created_at, "%Y-%m-%dT%H:%M:%SZ")
                if pr_object.updated_at:
                    pr_object.updated_at = datetime.strptime(pr_object.updated_at, "%Y-%m-%dT%H:%M:%SZ")
                if pr_object.merged_at:
                    pr_object.merged_at = datetime.strptime(pr_object.merged_at, "%Y-%m-%dT%H:%M:%SZ")
                if pr_object.closed_at:
                    pr_object.closed_at = datetime.strptime(pr_object.closed_at, "%Y-%m-%dT%H:%M:%SZ")

                pull_request_data = github_repo.map_pull_request_info(pr_object)

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
                    return synced_prs


def handle_github_event(connector_key, event_type, payload, channel=None):
    if event_type == 'push':
        return handle_github_repository_push(connector_key, payload, channel)
    if event_type == 'pull_request':
        return handle_github_pull_request_event(connector_key, payload, channel)
