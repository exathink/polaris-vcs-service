# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
import pytest

from datetime import datetime
from polaris.common import db
from polaris.common.enums import VcsIntegrationTypes
from polaris.repos.db.model import Organization, Repository
from polaris.repos.db.schema import commits,contributors, contributor_aliases
from polaris.integrations.db import model as integrations_model

test_account_key = uuid.uuid4()
test_organization_key = uuid.uuid4()
test_repository_key = uuid.uuid4()
test_contributor_key = uuid.uuid4().hex

test_repository_name = 'test-repo'
test_contributor_name = 'Joe Blow'


commit_common_fields = dict(
    commit_date=datetime.utcnow(),
    commit_date_tz_offset=0,
    committer_contributor_key=uuid.uuid4().hex,
    committer_contributor_name='Joe Blow',
    author_date=datetime.utcnow(),
    author_date_tz_offset=0,
    author_contributor_key=uuid.uuid4().hex,
    author_contributor_name='Billy Bob',
    parents=["0000", "0001"],
    stats=dict(
        files=10,
        lines=20,
        insertions=10,
        deletions=10
    ),
    created_at=datetime.utcnow()

)

repositories_common_fields = dict(
    name='New Test Repo',
    url="https://foo.bar.com",
    public=False,
    vendor='git',
    integration_type=VcsIntegrationTypes.github.value,
    description="A fancy new repo",
    source_id="10002",
    properties=dict(
        ssh_url='git@github.com:/foo.bar',
        homepage='https://www.github.com',
        default_branch='master',
    )
)




@pytest.yield_fixture()
def setup_org_repo(setup_schema):

    with db.orm_session() as session:
        session.expire_on_commit=False
        organization = Organization(
            organization_key=test_organization_key,
            name='test-org',
            public=False
        )
        repository = Repository(
                organization_key=test_organization_key,
                key=test_repository_key,
                name=test_repository_name
            )
        organization.repositories.append(
            repository
        )
        session.add(organization)
        session.flush()

    yield repository, organization

    db.connection().execute("delete from repos.source_file_versions")
    db.connection().execute("delete from repos.source_files")
    db.connection().execute("delete from repos.commits")
    db.connection().execute("delete from repos.contributor_aliases")
    db.connection().execute("delete from repos.contributors")
    db.connection().execute("delete from repos.repositories")
    db.connection().execute("delete from repos.organizations")





@pytest.yield_fixture()
def setup_commits(setup_org_repo):
    repository, organization = setup_org_repo

    with db.create_session() as session:
        contributor_id = session.connection.execute(
            contributors.insert(
                dict(
                    key=test_contributor_key,
                    name=test_contributor_name
                )
            )
        ).inserted_primary_key[0]

        contributor_alias_id = session.connection.execute(
            contributor_aliases.insert(
                dict(
                    alias='joe@blow.com',
                    key=uuid.uuid4().hex,
                    display_name=test_contributor_name,
                    contributor_id=contributor_id,
                    contributor_key=test_contributor_key,
                    contributor_name=test_contributor_name,
                )
            )
        ).inserted_primary_key[0]

        inserted_commits = [
            dict(
                repository_id=repository.id,
                key=uuid.uuid4().hex,
                source_commit_id='XXXX',
                commit_message = 'A Change. Fixes issue #1000',
                committer_alias_id = contributor_alias_id,
                author_alias_id = contributor_alias_id,
                **commit_common_fields
            ),
            dict(
                repository_id=repository.id,
                key=uuid.uuid4().hex,
                source_commit_id='YYYY',
                commit_message='Another Change',
                committer_alias_id=contributor_alias_id,
                author_alias_id=contributor_alias_id,
                **commit_common_fields
            )
                ]
        session.connection.execute(
            commits.insert(inserted_commits)
        )

    yield inserted_commits






@pytest.yield_fixture
def setup_connectors(setup_schema):
    github_connector_key = uuid.uuid4()

    with db.orm_session() as session:
        session.expire_on_commit = False
        session.add(
            integrations_model.Github(
                key=github_connector_key,
                name='test-github-connector',
                base_url='https://api.github.com',
                account_key=test_account_key,
                organization_key=test_organization_key,
                github_organization='exathink',
                oauth_access_token='foobar',
                state='enabled'
            )
        )


    yield dict(
        github=github_connector_key
    )

    db.connection().execute(f"delete from integrations.connectors")


repositories_common_fields = dict(
    name='New Test Repo',
    url="https://foo.bar.com",
    public=False,
    vendor='git',
    integration_type=VcsIntegrationTypes.github.value,
    description="A fancy new repo",
    source_id="10002",
    properties=dict(
        ssh_url='git@github.com:/foo.bar',
        homepage='https://www.github.com',
        default_branch='master',
    )
)

@pytest.yield_fixture
def setup_sync_repos(setup_org_repo, setup_connectors):
    organization, _ = setup_org_repo
    connectors = setup_connectors

    yield organization.key, connectors
