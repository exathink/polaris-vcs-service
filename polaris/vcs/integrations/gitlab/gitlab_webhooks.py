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


@webhook.route(f"/repository/push/<connector_key>/", methods=('GET', 'POST'))
def repository_push(connector_key):
    logger.info('Received webhook event @repository/push')

    event_type = request.json['object_kind']
    publish.gitlab_repository_event(event_type, connector_key, request.data)
    return ''


@webhook.route(f"/repository/webhooks/<connector_key>/", methods=('GET', 'POST'))
def repository_webhook(connector_key):
    logger.info('Received webhook event @repository/webhooks')

    event_type = request.json['object_kind']
    publish.gitlab_repository_event(event_type, connector_key, request.data)
    return ''
