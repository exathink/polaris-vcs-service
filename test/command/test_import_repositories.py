# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from unittest.mock import patch

from test.shared_fixtures import *
from polaris.vcs import commands
from polaris.messaging.test_utils import assert_topic_and_message
from polaris.messaging.topics import VcsTopic
from polaris.messaging.messages import RepositoriesImported

class TestImportRepositories:

    def it_returns_the_imported_repositories(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        repository_key = test_repository_key

        with patch('polaris.vcs.commands.publish.repositories_imported'):
            repositories = commands.import_repositories(organization_key, connectors['github'], [repository_key])
            assert len(repositories) == 1

    def it_publishes_the_repositories_imported_message(self, setup_sync_repos):
        organization_key, connectors = setup_sync_repos
        repository_key = test_repository_key

        with patch('polaris.vcs.messaging.publish.publish') as publish:
            commands.import_repositories(organization_key, connectors['github'], [repository_key])
            assert_topic_and_message(publish, VcsTopic, RepositoriesImported)
