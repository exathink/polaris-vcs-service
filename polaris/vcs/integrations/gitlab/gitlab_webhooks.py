# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from flask import Blueprint

logger = logging.getLogger('polaris.vcs.integrations.gitlab.webhook')

webhook = Blueprint('gitlab_webhooks', __name__)

webhook_paths = {
    'repository:push': '/repository/push'
}


@webhook.route(webhook_paths['repository:push'], methods=('GET', 'POST'))
def repository_push():
    logger.info('Received webhook: repository push')
    return 'ok'
