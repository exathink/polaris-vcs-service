# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from urllib import parse
from polaris.common.enums import VcsIntegrationTypes
from polaris.integrations.atlassian_connect import BitBucketBaseConnector
from polaris.utils.exceptions import ProcessingException
from polaris.common.enums import BitbucketPullRequestState

log = logging.getLogger('polaris.vcs.bitbucket_connector')


class BitBucketConnector(BitBucketBaseConnector):

    def __init__(self, connector):
        super().__init__(connector)
        self.base_url = f'{connector.base_url}'
        self.atlassian_account_key = connector.atlassian_account_key

    def test(self):
        fetch_repos_url = f'/2.0/repositories/{{{self.atlassian_account_key}}}'

        response = self.get(
            fetch_repos_url,
            headers={"Accept": "application/json"},
        )

        if response.ok:
            return True
        else:
            raise ProcessingException(f'Bitbucket Connector Test Failed: {response.text} ({response.status_code})')

    @staticmethod
    def get_next_result_url(next_page):
        if next_page is not None:
            page_parts = parse.urlsplit(next_page)
            return page_parts.path, parse.parse_qs(page_parts.query)
        else:
            return None, None

    @staticmethod
    def get_clone_url(clone_urls, scheme):
        for entry in clone_urls:
            if entry['name'] == scheme:
                return entry['href']

    def fetch_repositories(self, url=None):
        fetch_repos_url = url or f'/2.0/repositories/{{{self.atlassian_account_key}}}'
        params = None

        while fetch_repos_url is not None:
            response = self.get(
                fetch_repos_url,
                params=params,
                headers={"Accept": "application/json"},
            )

            if response.ok:
                result = response.json()
                yield result['values']
                fetch_repos_url, params = self.get_next_result_url(result.get('next'))
            else:
                log.error(
                    f'Bitbucket Fetch repositories failed: '
                    f'{self.connector.name}: {fetch_repos_url} {response.text} ({response.status_code})'
                )
                raise ProcessingException(
                    f'Bitbucket Fetch repositories failed: '
                    f'{response.text} ({response.status_code})'
                )

    def map_repository_info(self, repo):
        forked_from = repo.get('parent')

        return dict(
            name=repo['name'] if forked_from is None else f"{repo['full_name'].replace('/', ' <- ')}",
            url=self.get_clone_url(repo['links']['clone'], 'https'),
            public=not repo['is_private'],
            vendor='git',
            integration_type=VcsIntegrationTypes.bitbucket.value,
            description=repo['description'],
            source_id=repo['uuid'],
            polling=True,
            properties=dict(
                full_name=repo['full_name'],
                ssh_url=self.get_clone_url(repo['links']['clone'], 'ssh'),
                homepage=repo['website'],
                default_branch=repo['mainbranch']['name'] if repo['mainbranch'] else 'master',
                fork=forked_from  is not None,
                fork_source_id=forked_from.get('uuid') if forked_from is not None else None
            ),
        )

    def fetch_repositories_from_source(self, url=None):
        for repositories in self.fetch_repositories(url):
            yield [
                self.map_repository_info(repo)
                for repo in repositories
                if repo['scm'] == 'git'  # we dont support hg or sourcetree which are options in bitbucket
            ]


class PolarisBitBucketRepository:

    @staticmethod
    def create(repository, connector):
        if repository.integration_type == VcsIntegrationTypes.bitbucket.value:
            return BitBucketRepository(repository, connector)
        else:
            raise ProcessingException(f"Unknown integration_type: {repository.integration_type}")


class BitBucketRepository(PolarisBitBucketRepository):

    def __init__(self, repository, connector):
        self.repository = repository
        self.source_repo_id = repository.source_id
        self.last_updated = repository.latest_pull_request_update_timestamp
        self.state_mapping = dict(
            open=BitbucketPullRequestState.open.value,
            superseded=BitbucketPullRequestState.superseded.value,
            merged=BitbucketPullRequestState.merged.value,
            declined=BitbucketPullRequestState.declined.value
        )
        self.base_url = f'{connector.base_url}'
        self.atlassian_account_key = connector.atlassian_account_key
        self.connector = connector
        self.repo_url = f'/2.0/repositories/{{{self.atlassian_account_key}}}/{{{self.source_repo_id.strip("{}")}}}'

    def map_pull_request_info(self, pull_request):
        # TODO: Validate if merge_commit is the right attribute to identify merge status \
        #  Also validate closed date vs merged date
        source_merged_at = pull_request['updated_on'] if (
                    pull_request['merge_commit'] is not None and pull_request['closed_by'] is not None) else None
        source_closed_at = pull_request['updated_on'] if (
                    pull_request['state'].lower() == 'declined' and pull_request['closed_by'] is not None) else None
        end_date = source_merged_at if source_merged_at is not None else source_closed_at
        return dict(
            source_id=pull_request['id'],
            source_display_id=pull_request['id'],
            title=pull_request['title'],
            description=pull_request['description'],
            source_state=pull_request['state'].lower(),
            state=self.state_mapping[pull_request['state'].lower()],
            source_created_at=pull_request['created_on'],
            source_last_updated=pull_request['updated_on'],
            source_merge_status='can_be_merged' if pull_request['merge_commit'] is not None else None,
            source_merged_at=source_merged_at,
            source_closed_at=source_closed_at,
            end_date=end_date,
            source_branch=pull_request['source']['branch']['name'],
            target_branch=pull_request['destination']['branch']['name'],
            source_repository_source_id=pull_request['source']['repository']['uuid'],
            target_repository_source_id=pull_request['destination']['repository']['uuid'],
            web_url=pull_request['links']['html']['href']
        )

    def fetch_pull_requests(self):
        fetch_pull_requests_url = f'/2.0/repositories/{{{self.atlassian_account_key}}}/{{{self.source_repo_id.strip("{}")}}}/pullrequests'
        params = dict(
            state='open',
            sort='-updated_on'
        )

        while fetch_pull_requests_url is not None:
            response = self.connector.get(
                fetch_pull_requests_url,
                params=params,
                headers={"Accept": "application/json"},
            )

            if response.ok:
                result = response.json()
                yield result['values']
                fetch_pull_requests_url, params = self.connector.get_next_result_url(result.get('next'))
            else:
                log.error(
                    f'Bitbucket fetch pull requests failed: '
                    f'{self.connector.name}: {fetch_pull_requests_url} {response.text} ({response.status_code})'
                )
                raise ProcessingException(
                    f'Bitbucket fetch pull requests failed: '
                    f'{response.text} ({response.status_code})'
                )

    def fetch_pull_request(self, source_id):
        fetch_pull_requests_url = f'/2.0/repositories/{{{self.atlassian_account_key}}}/{{{self.source_repo_id.strip("{}")}}}/pullrequests/{source_id}'

        response = self.connector.get(
            fetch_pull_requests_url,
            headers={"Accept": "application/json"},
        )

        if response.ok:
            yield [response.json()]

        else:
            log.error(
                f'Bitbucket fetch pull request failed: '
                f'{self.connector.name}: {fetch_pull_requests_url} {response.text} ({response.status_code})'
            )
            raise ProcessingException(
                f'Bitbucket fetch pull request failed: '
                f'{response.text} ({response.status_code})'
            )

    def fetch_pull_requests_from_source(self, source_id=None):
        if source_id is None:
            for pull_requests in self.fetch_pull_requests():
                yield [
                    self.map_pull_request_info(pr)
                    for pr in pull_requests
                ]

        else:
            for pull_requests in self.fetch_pull_request(source_id):
                yield [
                    self.map_pull_request_info(pr)
                    for pr in pull_requests
                ]

    def fetch_repository_forks(self):
        fetch_forks_url = f"{self.repo_url}/forks"
        yield from self.connector.fetch_repositories_from_source(url=fetch_forks_url)