# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging
from flask import Blueprint, request
from polaris.vcs.messaging import publish

logger = logging.getLogger('polaris.vcs.integrations.github.webhook')

webhook = Blueprint('github_webhooks', __name__)


@webhook.route(f"/repository/webhooks/<connector_key>/", methods=('GET', 'POST'))
def repository_webhook(connector_key):
    logger.info('Received webhook event @repository/webhooks')
    req_data = request.json
    if req_data is not None:
        event_type = None
        if req_data.get('refs'):
            event_type = 'push'
        if req_data.get('pull_request'):
            event_type = 'pull_request'

        if event_type is not None:
            publish.github_repository_event(event_type, connector_key, req_data)

    return ''
