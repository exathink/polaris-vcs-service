# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal
import logging
from polaris.repos.db.model import repositories, Repository


log = logging.getLogger('polaris.vcs.db.impl.pull_requests')


def import_pull_requests(session, organization_key, repository_key, source_pull_requests):
    if organization_key and repository_key:
        repository = Repository.find_by_repository_key(repository_key)
        source_repo_id = repository.source_id
        # create a temp table for pull requests and insert source_pull_requests
        # insert into the repo.pull_requests table
