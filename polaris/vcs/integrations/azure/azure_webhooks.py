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

webhook = Blueprint('azure_webhooks', __name__)


@webhook.route(f"/repository/webhooks/<connector_key>/", methods=('GET', 'POST'))
def repository_webhook(connector_key):
    logger.info('Received webhook event @repository/webhooks')
    req_data = request.json
    if req_data is not None:
       logger.info(req_data)

    return ''


@webhook.route(f"/webhooks/ping/", methods=('GET', 'POST'))
def ping():
    logger.info('Received ping')

    return 'ok'
