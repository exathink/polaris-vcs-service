# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
import pytest

from datetime import datetime, timedelta
from polaris.common import db
from polaris.common.enums import VcsIntegrationTypes
from polaris.repos.db.model import Organization, Repository
from polaris.repos.db.schema import commits, contributors, contributor_aliases, RepositoryImportState
from polaris.integrations.db import model as integrations_model

test_account_key = uuid.uuid4()
test_organization_key = uuid.uuid4()
test_repository_key = uuid.uuid4()
test_repository_source_id = "1000"

test_contributor_key = uuid.uuid4().hex

test_repository_name = 'test-repo'
test_contributor_name = 'Joe Blow'

github_connector_key = uuid.uuid4()
gitlab_connector_key = uuid.uuid4()
bitbucket_connector_key = uuid.uuid4()

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
    polling=True,
    properties=dict(
        ssh_url='git@github.com:/foo.bar',
        homepage='https://www.github.com',
        default_branch='master',
    )
)

pull_requests_common_fields = dict(
    source_id='61296045',
    source_display_id='69',
    title="PO-178 Graphql API updates.",
    description="PO-178",
    source_state="merged",
    state="merged",
    source_created_at="2020-06-11T18:56:59.410Z",
    source_last_updated="2020-06-11T18:57:08.777Z",
    source_merge_status="can_be_merged",
    source_merged_at="2020-06-11T18:57:08.818Z",
    source_branch="PO-178",
    target_branch="master",
    source_repository_source_id="1000",
    target_repository_source_id="1000",
    web_url="https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
)


@pytest.fixture
def cleanup():
    yield

    db.connection().execute("delete from repos.source_file_versions")
    db.connection().execute("delete from repos.source_files")
    db.connection().execute("delete from repos.commits")
    db.connection().execute("delete from repos.contributor_aliases")
    db.connection().execute("delete from repos.contributors")
    db.connection().execute("delete from repos.pull_requests")
    db.connection().execute("delete from repos.repositories")
    db.connection().execute("delete from repos.organizations")


@pytest.fixture()
def setup_org_repo(setup_schema, cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
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
            import_state=RepositoryImportState.CHECK_FOR_UPDATES,
            last_imported=datetime.utcnow(),
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


@pytest.fixture()
def setup_org_repo_bitbucket(setup_schema, cleanup):
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
            source_id=test_repository_source_id,
            import_state=RepositoryImportState.CHECK_FOR_UPDATES,
            last_imported=datetime.utcnow(),
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


@pytest.fixture()
def setup_org_repo_gitlab(setup_schema, cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        organization = Organization(
            organization_key=test_organization_key,
            name='test-org',
            public=False
        )
        repository = Repository(
            connector_key=gitlab_connector_key,
            organization_key=test_organization_key,
            key=test_repository_key,
            name=test_repository_name,
            source_id=test_repository_source_id,
            import_state=RepositoryImportState.CHECK_FOR_UPDATES,
            last_imported=datetime.utcnow(),
            description='A neat new repo',
            integration_type=VcsIntegrationTypes.gitlab.value,
            url='https://foo.bar.com'

        )
        organization.repositories.append(
            repository
        )
        session.add(organization)
        session.flush()

    yield repository, organization


@pytest.fixture()
def setup_org_repo_no_connector(setup_schema, cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        organization = Organization(
            organization_key=test_organization_key,
            name='test-org',
            public=False
        )
        repository = Repository(
            organization_key=test_organization_key,
            key=test_repository_key,
            name=test_repository_name,
            source_id=test_repository_source_id,
            import_state=0,
            description='A neat new repo without a connector',
            integration_type=VcsIntegrationTypes.github.value,
            url='https://foo.bar.com'

        )
        organization.repositories.append(
            repository
        )
        session.add(organization)
        session.flush()

    yield repository, organization


@pytest.fixture()
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
                commit_message='A Change. Fixes issue #1000',
                committer_alias_id=contributor_alias_id,
                author_alias_id=contributor_alias_id,
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


@pytest.fixture
def setup_connectors(setup_schema):
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
        session.add(
            integrations_model.Gitlab(
                key=gitlab_connector_key,
                name='test-gitlab-connector',
                personal_access_token='XXXXXX',
                base_url='https://gitlab.com',
                account_key=test_account_key,
                organization_key=test_organization_key,
                webhook_secret='YYYYYY',
                state='enabled'
            )
        )
        session.add(
            integrations_model.AtlassianConnect(
                key=bitbucket_connector_key,
                name='test-bitbucket-connector',
                base_url='https://bitbucket.org',
                account_key=test_account_key,
                organization_key=test_organization_key,
                state='enabled'
            )
        )

    yield dict(
        github=github_connector_key,
        gitlab=gitlab_connector_key
    )

    db.connection().execute(f"delete from integrations.connectors")


@pytest.fixture
def setup_sync_repos(setup_org_repo, setup_connectors):
    repository, organization = setup_org_repo
    connectors = setup_connectors

    yield organization.organization_key, connectors


@pytest.fixture
def setup_sync_repos_disabled(setup_org_repo, setup_connectors):
    repository, organization = setup_org_repo
    connectors = setup_connectors

    yield organization.organization_key, connectors, repository


@pytest.fixture
def setup_sync_repos_gitlab(setup_org_repo_gitlab, setup_connectors):
    repository, organization = setup_org_repo_gitlab
    connectors = setup_connectors

    yield organization.organization_key, connectors


@pytest.fixture
def setup_sync_repos_gitlab_disabled(setup_org_repo_gitlab, setup_connectors):
    repository, organization = setup_org_repo_gitlab
    connectors = setup_connectors


    yield organization.organization_key, connectors, repository


@pytest.fixture
def setup_sync_repos_bitbucket(setup_org_repo_bitbucket, setup_connectors):
    repository, organization = setup_org_repo_bitbucket
    connectors = setup_connectors

    yield organization.organization_key, connectors


@pytest.fixture
def setup_sync_repos_bitbucket_disabled(setup_org_repo_bitbucket, setup_connectors):
    repository, organization = setup_org_repo_bitbucket
    connectors = setup_connectors



    yield organization.organization_key, connectors, repository


@pytest.fixture
def setup_repo_waiting_for_update(setup_org_repo):
    repository, organization = setup_org_repo
    with db.orm_session() as session:
        session.add(repository)
        repository.import_state = RepositoryImportState.CHECK_FOR_UPDATES

    yield repository, organization
