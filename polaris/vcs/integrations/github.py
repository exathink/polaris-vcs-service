# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from datetime import datetime, timedelta
from polaris.integrations.github import GithubConnector
from polaris.utils.exceptions import ProcessingException
from polaris.common.enums import VcsIntegrationTypes, GithubPullRequestState

logger = logging.getLogger('polaris.vcs.integrations.github')


class GithubRepositoriesConnector(GithubConnector):

    def __init__(self, connector):
        super().__init__(connector)

    def map_repository_info(self, repo):
        return dict(
            name=repo.name,
            url=repo.html_url,
            public=not repo.private,
            vendor='git',
            integration_type=VcsIntegrationTypes.github.value,
            description=repo.description,
            source_id=repo.id,
            polling=True,
            properties=dict(
                ssh_url=repo.ssh_url,
                homepage=repo.homepage,
                default_branch=repo.default_branch,
            ),
        )

    def fetch_repositories(self):
        if self.access_token is not None:
            github = self.get_github_client()
            organization = github.get_organization(self.github_organization)
            if organization is not None:
                return organization.get_repos()
        else:
            raise ProcessingException("No access token found this Github Connector. Cannot continue.")

    def fetch_repositories_from_source(self):
        repos_paginator = self.fetch_repositories()
        while repos_paginator._couldGrow():
            yield [
                self.map_repository_info(repo)
                for repo in repos_paginator._fetchNextPage()
                if not repo.archived
            ]


class PolarisGithubRepository:

    @staticmethod
    def create(repository, connector):
        if repository.integration_type == VcsIntegrationTypes.github.value:
            return GithubRepository(repository, connector)
        else:
            raise ProcessingException(f"Unknown integration type: {repository.integration_type}")


# FIXME: Hardcoded value for initial import days
INITIAL_IMPORT_DAYS = 90
class GithubRepository(PolarisGithubRepository):

    def __init__(self, repository, connector):
        self.repository = repository
        self.source_repo_id = repository.source_id
        self.last_updated = repository.latest_pull_request_update_timestamp \
            if repository.latest_pull_request_update_timestamp is not None \
            else datetime.utcnow()-timedelta(days=INITIAL_IMPORT_DAYS)
        self.connector = connector
        self.access_token = connector.access_token
        self.state_mapping = dict(
            open=GithubPullRequestState.open.value,
            closed=GithubPullRequestState.closed.value,
            merged=GithubPullRequestState.merged.value
        )

    def map_pull_request_info(self, pull_request):
        if pull_request.merged_at is not None:
            pr_end_date = pull_request.merged_at.strftime("%Y-%m-%d %H:%M:%S")
        elif pull_request.closed_at is not None:
            pr_end_date = pull_request.closed_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            pr_end_date = None
        return dict(
            source_id=pull_request.id,
            source_display_id=pull_request.number,
            title=pull_request.title,
            description=pull_request.body,
            source_state=pull_request.state,
            state=self.state_mapping['merged'] if pull_request.merged_at is not None else self.state_mapping[pull_request.state],
            source_created_at=pull_request.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            source_last_updated=pull_request.updated_at.strftime("%Y-%m-%d %H:%M:%S") if pull_request.updated_at else None,
            # TODO: Figure out how to determine merge status.
            source_merge_status=None,
            source_merged_at=pull_request.merged_at.strftime("%Y-%m-%d %H:%M:%S") if pull_request.merged_at else None,
            source_closed_at=pull_request.closed_at.strftime("%Y-%m-%d %H:%M:%S") if pull_request.closed_at else None,
            end_date=pr_end_date,
            source_branch=pull_request.head.ref,
            target_branch=pull_request.base.ref,
            source_repository_source_id=pull_request.head.repo.id,
            target_repository_source_id=pull_request.base.repo.id,
            web_url=pull_request.url
        )

    def fetch_pull_requests_from_source(self):
        if self.access_token is not None:
            github = self.connector.get_github_client()
            repo = github.get_repo(int(self.repository.source_id))
            # TODO: There is no 'since' parameter so fetching all PRs. \
            #  Checking during iteration on pages for last updated PR
            prs_iterator = repo.get_pulls(
                state='all',
                sort='updated',
                direction='desc'
            )

            fetched_upto_last_update = False
            while prs_iterator._couldGrow() and not fetched_upto_last_update:
                pull_requests = []
                for pr in prs_iterator._fetchNextPage():
                    if pr.updated_at < self.last_updated:
                        fetched_upto_last_update = True
                        break
                    else:
                        pull_requests.append(self.map_pull_request_info(pr))
                yield pull_requests


