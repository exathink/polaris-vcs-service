# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from flask import Blueprint, request
from polaris.vcs.messaging import publish

logger = logging.getLogger('polaris.vcs.integrations.gitlab.webhook')

webhook = Blueprint('gitlab_webhooks', __name__)

webhook_paths = {
    'repository:push': '/repository/push',
    'pull_request:push': '/merge_request/push'
}


@webhook.route(f"{webhook_paths['repository:push']}/<connector_key>/", methods=('GET', 'POST'))
def repository_push(connector_key):
    logger.info('Received webhook: repository push')
    # FIXME: Added some hacks to use the same endpoint to publish different events internally. Modify after discussion.
    if request.json['event_type'] == 'push':
        event_type = 'repository:push'
    elif request.json['event_type'] == 'merge_request':
        event_type = 'pull_request:push'
    else:
        event_type = 'unknown'
    publish.gitlab_repository_event(event_type, connector_key, request.data)
    return ''


# @webhook.route(f"{webhook_paths['pull_request:push']}/<connector_key>/", methods=('GET', 'POST'))
# def merge_request_push(connector_key):
#     logger.info('Received webhook: pull_request push')
#
#     publish.gitlab_repository_event('pull_request:push', connector_key, request.data)
#     return ''
