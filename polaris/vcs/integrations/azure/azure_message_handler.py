# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json
import logging
import re
from polaris.common import db
from polaris.repos.db.model import Repository, PullRequest
from polaris.vcs.messaging import publish
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.vcs.integrations.azure.azure_message_handler')


def handle_azure_repository_push(connector_key, payload, channel=None):
    event = json.loads(payload)
    if 'resource' in event:
        resource_url = event.get('resource').get('url')
        match = re.fullmatch(r"^https://.*/repositories/(?P<repo_source_id>.*)/pushes/.*$", resource_url)
        if match is not None:
            publish.remote_repository_push_event(connector_key, match['repo_source_id'], channel)
        else:
            logger.error(f"Invalid resource_url {resource_url} received for azure repository push")
            raise ProcessingException(f"Invalid resource_url {resource_url} received for azure repository push")


def handle_azure_pull_request_event(connector_key, payload, channel=None):
    event = json.loads(payload)
    if 'resource' in event:
        resource_url = event.get('resource').get('url')
        match = re.fullmatch(
            r"^https://.*/repositories/(?P<repo_source_id>.*)/pullRequests/(?P<pr_source_id>.*)$",
            resource_url
        )
        if match is not None:
            with db.orm_session() as session:
                repo_source_id = match['repo_source_id']
                repo = Repository.find_by_connector_key_source_id(session, connector_key, repo_source_id )
                if repo is not None:
                    publish.sync_pull_request(
                        repo.organization_key,
                        repo.key,
                        pull_request_key=None,
                        pull_request_source_id=match['pr_source_id']
                    )
                else:
                    raise ProcessingException(f'handle_azure_pull_request: Could not find repo with source id'
                                              f' {repo_source_id} for connector {connector_key}')

            publish.remote_repository_push_event(connector_key, match['repo_source_id'], channel)
        else:
            logger.error(f"Invalid resource_url {resource_url} received for azure repository push")
            raise ProcessingException(f"Invalid resource_url {resource_url} received for azure repository push")


def handle_azure_event(connector_key, event_type, payload, channel=None):
    if event_type == 'git.push':
        return handle_azure_repository_push(connector_key, payload, channel)
    elif 'pullrequest' in event_type:
        return handle_azure_pull_request_event(connector_key, payload, channel)
    else:
        raise ProcessingException(f"Unrecognized azure event type {event_type}")