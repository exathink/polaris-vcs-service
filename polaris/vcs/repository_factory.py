# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.common.enums import VcsIntegrationTypes
from polaris.utils.exceptions import ProcessingException
from polaris.vcs.integrations.gitlab import PolarisGitlabRepository
from polaris.common import db
from polaris.repos.db.model import Repository


def get_provider_impl(repository_key, join_this=None):
    with db.orm_session(join_this) as session:
        repository = Repository.find_by_repository_key(session, repository_key)
        if repository:
            if repository.integration_type == VcsIntegrationTypes.gitlab.value:
                repository_impl = PolarisGitlabRepository.create(repository)
            else:
                raise ProcessingException(
                    f'Could not determine repository_implementation for repository_key {repository.key}'
                )
            return repository_impl

        else:
            raise ProcessingException(
                f'Could not find repository with key {repository_key}'
            )
