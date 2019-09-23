# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging
import json
from polaris.vcs.messaging import publish

logger = logging.getLogger('polaris.vcs.integrations.bitbucket.message_handler')


def handle_atlassian_connect_repository_event(connector_key, event_type, event):
    if event_type == 'repo:push':
        return handle_repo_push(connector_key, event)


def handle_repo_push(connector_key, event):
    payload = json.loads(event)
    repo_source_id = payload['data']['repository']['uuid']
    logger.info(f'Received repo:push event for bitbucket connector {connector_key}')

    publish.remote_repository_push_event(connector_key, repo_source_id)
