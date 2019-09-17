# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json
from sqlalchemy import select, func
from polaris.vcs.messaging.messages import AtlassianConnectRepositoryEvent
from polaris.messaging.test_utils import mock_channel, fake_send
from polaris.vcs.messaging.subscribers.vcs_topic_subscriber import VcsTopicSubscriber
from test.shared_fixtures import *
from polaris.repos.db.schema import RepositoryImportState
from polaris.common.enums import VcsIntegrationTypes

bitbucket_connector_key = uuid.uuid4()
bitbucket_repo_source_id = uuid.uuid4()


@pytest.yield_fixture()
def setup_repo_waiting_for_update(setup_schema, cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        organization = Organization(
            organization_key=test_organization_key,
            name='test-org',
            public=False
        )
        repository = Repository(
            connector_key=bitbucket_connector_key,
            organization_key=test_organization_key,
            key=test_repository_key,
            name=test_repository_name,
            source_id=bitbucket_repo_source_id,
            import_state=RepositoryImportState.CHECK_FOR_UPDATES,
            description='A neat new repo',
            integration_type=VcsIntegrationTypes.bitbucket.value,
            url='https://foo.bar.com'

        )
        organization.repositories.append(
            repository
        )
        session.add(organization)
        session.flush()

    yield repository, organization


class TestRepositoryPush:

    def it_processes_the_message_correctly(self, setup_repo_waiting_for_update):
        repo, org = setup_repo_waiting_for_update

        payload = dict(
            atlassian_connector_key=bitbucket_connector_key,
            atlassian_event_type="repo:push",
            atlassian_event=json.dumps(dict(
                data=dict(
                    repository=dict(
                        uuid=f"{bitbucket_repo_source_id}"
                    )
                )
            ))
        )

        channel = mock_channel()
        message = fake_send(AtlassianConnectRepositoryEvent(send=payload))
        result = VcsTopicSubscriber(channel).dispatch(channel, message)
        assert result['success']
        assert db.connection().execute(
            f"select import_state from repos.repositories where source_id='{bitbucket_repo_source_id}'"
        ).scalar() == RepositoryImportState.UPDATE_READY



