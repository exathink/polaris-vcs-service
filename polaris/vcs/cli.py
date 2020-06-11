# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

import argh

from polaris.common import db
from polaris.messaging.topics import VcsTopic
from polaris.messaging.messages import RepositoriesImported
from polaris.messaging.utils import publish
from polaris.common.enums import VcsIntegrationTypes

from polaris.utils.logging import config_logging


logger = logging.getLogger('polaris.vcs_service.cli')

db.init()

def sync_pull_requests(organization_key=None):
    imported_repositories = [
        dict(
            name='polaris-analytics-service',
            key='f6b81d4f-bee5-44a8-9ebc-662157842839',
            url='https://foo.bar.com',
            description='Analytics repo',
            public=False,
            integration_type=VcsIntegrationTypes.gitlab.value
        ),
    ]
    publish(VcsTopic,
            RepositoriesImported(send=dict(organization_key=organization_key, imported_repositories=imported_repositories)))


if __name__ == '__main__':
    config_logging(
        suppress=['requests.packages.urllib3.connectionpool']
    )

    argh.dispatch_commands([
        sync_pull_requests
    ])
