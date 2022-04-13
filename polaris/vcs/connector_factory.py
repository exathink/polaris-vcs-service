# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.common import db
from polaris.common.enums import ConnectorType
from polaris.integrations.db.api import find_connector, find_connector_by_name
from polaris.utils.exceptions import ProcessingException
from polaris.vcs.integrations.github import GithubRepositoriesConnector
from polaris.vcs.integrations.atlassian import BitBucketConnector
from polaris.vcs.integrations.gitlab import GitlabRepositoriesConnector
from polaris.vcs.integrations.azure import AzureRepositoriesConnector

def get_connector(connector_name=None, connector_key=None, join_this=None):
    with db.orm_session(join_this) as session:
        if connector_key is not None or connector_name is not None:
            if connector_key is not None:
                connector = find_connector(connector_key, join_this=session)
            else:
                connector = find_connector_by_name(connector_name, join_this=session)
            if connector:
                if connector.type == ConnectorType.github.value:
                    return GithubRepositoriesConnector(connector)
                elif connector.type == ConnectorType.atlassian.value:
                    return BitBucketConnector(connector)
                elif connector.type == ConnectorType.gitlab.value:
                    return GitlabRepositoriesConnector(connector)
                elif connector.type == ConnectorType.azure.value:
                    return AzureRepositoriesConnector(connector)
                else:
                    raise ProcessingException(f'No Repositories connector registered for connector type: {connector.type} '
                                              f'Connector Key was {connector_key}')

            else:
                raise ProcessingException(f'Cannot find connector for connector_key {connector_key}')
