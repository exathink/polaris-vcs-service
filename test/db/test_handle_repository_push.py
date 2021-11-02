# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
from test.shared_fixtures import *
from polaris.common import db
from polaris.vcs.db import api
from polaris.repos.db.schema import RepositoryImportState


@pytest.fixture()
def setup_bitbucket_repos(setup_schema, cleanup):

    with db.orm_session() as session:
        session.expire_on_commit=False
        organization = Organization(
            organization_key=test_organization_key,
            name='test-org',
            public=False
        )
        repository = Repository(
                connector_key=github_connector_key,
                organization_key=test_organization_key,
                key=test_repository_key,
                name=test_repository_name,
                source_id=test_repository_source_id,
                import_state=0,
                description='A neat new repo',
                integration_type=VcsIntegrationTypes.github.value,
                url='https://foo.bar.com'

            )
        organization.repositories.append(
            repository
        )
        session.add(organization)
        session.flush()

    yield repository, organization

class TestHandleRepositoryPush:

    def it_updates_the_import_state_if_it_is_waiting_for_updates(self, setup_repo_waiting_for_update):
        repo, org = setup_repo_waiting_for_update

        result = api.handle_remote_repository_push(github_connector_key, test_repository_source_id)
        assert result['success']
        assert db.connection().execute(
            f"select import_state from repos.repositories where key='{repo.key}'"
        ).scalar() == RepositoryImportState.UPDATE_READY



