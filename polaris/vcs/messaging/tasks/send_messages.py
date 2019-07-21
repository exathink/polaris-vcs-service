# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import argh
from polaris.common import db
from polaris.repos.intake.service.messaging import publish
from polaris.utils.logging import config_logging

from polaris.vcs.messaging.tasks import commit_history_imported
from polaris.vcs.messaging.tasks import commit_details_imported


def sync_commits(organization_key=None, repository_key=None):
    commit_history_imported(organization_key=organization_key, repository_key=repository_key)
    commit_details_imported(organization_key=organization_key, repository_key=repository_key)


if __name__ == "__main__":
    db.init()
    publish.init()
    config_logging()

    argh.dispatch_commands([
        commit_history_imported,
        commit_details_imported,
        sync_commits
    ])