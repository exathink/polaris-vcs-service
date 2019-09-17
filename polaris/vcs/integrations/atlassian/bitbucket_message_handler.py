# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import json
from polaris.repos.db.model import Repository
from polaris.vcs.db import api
from polaris.common import db
from polaris.utils.exceptions import ProcessingException


def handle_atlassian_connect_repository_event(connector_key, event_type, event):
    if event_type == 'repo:push':
        return handle_repo_push(connector_key, event)


def handle_repo_push(connector_key, event):
    payload = json.loads(event)
    repo_source_id = payload['data']['repository']['uuid']
    repo = find_repository(connector_key, repo_source_id)

    if repo is not None:
        return api.handle_repository_push(repo.organization_key, repo.key)
    else:
        raise ProcessingException(f"Could not find repository with connectory key "
                                  f"{connector_key} and source_id {repo_source_id}")


def find_repository(connector_key, source_id):
    with db.orm_session() as session:
        session.expire_on_commit = False
        return Repository.find_by_connector_key_source_id(session, connector_key, source_id)
