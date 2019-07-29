# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch

from test.shared_fixtures import *
from graphene.test import Client
from polaris.vcs.service.graphql import schema

class TestImportRepositoriesMutation:

    def it_returns_the_imported_repository_keys(self, setup_sync_repos):
        organization_key, _ = setup_sync_repos
        repository_key = test_repository_key

        client = Client(schema)

        with patch('polaris.vcs.messaging.publish.publish'):
            response = client.execute("""
                mutation importRepositories($importRepositoriesInput: ImportRepositoriesInput!) {
                    importRepositories(importRepositoriesInput: $importRepositoriesInput) {
                        success
                        importedRepositoryKeys
                    }
                }
            """, variable_values=dict(
                importRepositoriesInput=dict(
                    organizationKey=str(organization_key),
                    repositoryKeys=[str(repository_key)]
                )
            ))

        assert response['data']
        assert response['data']['importRepositories']['success']
        assert len(response['data']['importRepositories']['importedRepositoryKeys']) == 1