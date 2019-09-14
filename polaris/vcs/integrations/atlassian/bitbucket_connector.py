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

    def fetch_repositories(self):
        fetch_repos_url = f'/2.0/repositories/{{{self.atlassian_account_key}}}'
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
                fetch_repos_url, params = self.get_next_result_url(result['next'])
            else:
                log.error(
                    f'Bitbucket Fetch repositories failed: '
                    f'{self.connector.name }: {fetch_repos_url} {response.text} ({response.status_code})'
                )
                raise ProcessingException(
                    f'Bitbucket Fetch repositories failed: '
                    f'{response.text} ({response.status_code})'
                )

    def map_repository_info(self, repo):
        return dict(
            name=repo['name'],
            url=self.get_clone_url(repo['links']['clone'], 'https'),
            public=not repo['is_private'],
            vendor='git',
            integration_type=VcsIntegrationTypes.bitbucket.value,
            description=repo['description'],
            source_id=repo['uuid'],
            properties=dict(
                full_name=repo['full_name'],
                ssh_url=self.get_clone_url(repo['links']['clone'], 'ssh'),
                homepage=repo['website'],
                default_branch=repo['mainbranch']['name']
            ),
        )

    def fetch_repositories_from_source(self):
        for repositories in self.fetch_repositories():
            yield [
                self.map_repository_info(repo)
                for repo in repositories
                if repo['scm'] == 'git'  # we dont support hg or sourcetree which are options in bitbucket
            ]
