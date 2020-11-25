# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy.exc import SQLAlchemyError

from polaris.common import db

from .impl import commits, repositories, pull_requests


# Repositories
def sync_repositories(organization_key, connector_key, source_repositories):
    try:
        with db.orm_session() as session:
            return repositories.sync_repositories(session, organization_key, connector_key, source_repositories)
    except SQLAlchemyError as exc:
        return db.process_exception("Sync Repositories", exc)
    except Exception as e:
        return db.failure_message('Sync Repositories', e)


def import_repositories(organization_key, repository_keys):
    try:
        with db.orm_session() as session:
            return repositories.import_repositories(session, organization_key, repository_keys)
    except SQLAlchemyError as exc:
        return db.process_exception("Import Repositories", exc)
    except Exception as e:
        return db.failure_message('Import Repositories', e)


def sync_pull_requests(repository_key, source_pull_requests):
    try:
        with db.orm_session() as session:
            return pull_requests.sync_pull_requests(session, repository_key, source_pull_requests)
    except SQLAlchemyError as exc:
        return db.process_exception("Import Pull Requests", exc)
    except Exception as e:
        return db.failure_message('Import Pull Requests', e)


def register_webhooks(repository_key, webhook_info, join_this=None):
    try:
        with db.orm_session(join_this) as session:
            return repositories.register_webhooks(session, repository_key, webhook_info)
    except SQLAlchemyError as exc:
        return db.process_exception("Register Webhook", exc)
    except Exception as e:
        return db.failure_message('Register Webhook', e)


def handle_remote_repository_push(connector_key, repository_source_id):
    try:
        with db.orm_session() as session:
            return repositories.handle_remote_repository_push(session, connector_key, repository_source_id)

    except SQLAlchemyError as exc:
        return db.process_exception("Handle repository push", exc)
    except Exception as e:
        return db.failure_message('Handle repository push', e)


# Commits
def ack_commits_created(commit_keys):
    try:
        with db.create_session() as session:
            return commits.ack_commits_created(session, commit_keys)
    except SQLAlchemyError as exc:
        return db.process_exception("Ack Commits Created", exc)
    except Exception as e:
        return db.failure_message('Ack Commits Created', e)


def ack_commits_details_created(commit_keys):
    try:
        with db.create_session() as session:
            return commits.ack_commit_details_created(session, commit_keys)
    except SQLAlchemyError as exc:
        return db.process_exception("Ack Commits Created", exc)
    except Exception as e:
        return db.failure_message('Ack Commits Created', e)
