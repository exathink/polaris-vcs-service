# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json

from polaris.vcs.messaging import publish


def handle_gitlab_event(connector_key, event_type, payload, channel=None):
    if event_type == 'repository:push':
        return handle_gitlab_repository_push(connector_key, payload, channel)


def handle_gitlab_repository_push(connector_key, payload, channel=None):
    event = json.loads(payload)
    repo_source_id = event.get('project_id')

    publish.remote_repository_push_event(connector_key, repo_source_id, channel)
