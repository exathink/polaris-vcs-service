# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy.exc import SQLAlchemyError

from polaris.common import db

from .impl import commits, repositories


# Repositories
def sync_repositories(organization_key, source_repositories):
    try:
        with db.orm_session() as session:
            return repositories.sync_repositories(session, organization_key, source_repositories)
    except SQLAlchemyError as exc:
        return db.process_exception("Sync Repositories", exc)
    except Exception as e:
        return db.failure_message('Sync Repositories', e)


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
