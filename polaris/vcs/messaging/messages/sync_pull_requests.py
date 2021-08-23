# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from marshmallow import fields

from polaris.messaging.messages import Command


class SyncPullRequests(Command):
    message_type = 'vcs.sync_pull_requests'

    organization_key = fields.String(required=True)
    repository_key = fields.String(required=True)
    pull_request_key = fields.String(required=False)





