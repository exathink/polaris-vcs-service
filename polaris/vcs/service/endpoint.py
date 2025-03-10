# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import sys
from flask_compress import Compress
import logging

from polaris.utils.config import get_config_provider
from polaris.flask.common import PolarisSecuredService
from polaris.flask import gql
from polaris.utils.logging import config_logging
from polaris.messaging.topics import WorkItemsTopic, ConnectorsTopic
from polaris.messaging.utils import init_topics_to_publish
from polaris.vcs.integrations.atlassian import bitbucket_atlassian_connect
from polaris.vcs.integrations.gitlab import gitlab_webhooks
from polaris.vcs.integrations.github import github_webhooks
from polaris.vcs.integrations.azure import azure_webhooks

from polaris.vcs.service import graphql


config_logging()


class PolarisVcsService(PolarisSecuredService):
    def __init__(self, import_name, db_url, db_connect_timeout=30, models=None,
                 public_paths=None, **kwargs):
        super(PolarisVcsService, self).__init__(
            import_name, db_url, db_connect_timeout,
            models=models,
            public_paths=public_paths,
            **kwargs
        )
        self.public_paths.extend([])


config_provider = get_config_provider()
app = PolarisVcsService(
    __name__,
    db_url=config_provider.get('POLARIS_DB_URL'),
    public_paths=[
        '/atlassian_connect'
    ],
    strict_security=bool(config_provider.get('STRICT_SECURITY', None)),
    force_https=bool(config_provider.get('FORCE_HTTPS', None))
)

if config_provider.get('DEBUG_SQL') == 'true':
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Register endpoints
app.register_blueprint(gql.api, url_prefix='/graphql', schema=graphql.schema)
app.register_blueprint(gitlab_webhooks.webhook, url_prefix='/gitlab')
app.register_blueprint(github_webhooks.webhook, url_prefix='/github')
app.register_blueprint(azure_webhooks.webhook, url_prefix='/azure')

bitbucket_atlassian_connect.init_connector(app)

# Make sure topics we interact with are available.
init_topics_to_publish(WorkItemsTopic, ConnectorsTopic)

if app.env == 'production':
    app.config['COMPRESS_MIMETYPES'] = ['application/json', 'application/javascript']

    compress = Compress()
    compress.init_app(app)


# for dev mode use only.
if __name__ == "__main__":
    # Pycharm optimized settings.
    # Debug is turned off by default (use PyCharm debugger)
    # reloader is turned on by default so that we can get hot code reloading
    DEBUG = '--debug' in sys.argv
    RELOAD = '--no-reload' not in sys.argv
    app.run(host='0.0.0.0', port=8100, debug=DEBUG, use_reloader=RELOAD)