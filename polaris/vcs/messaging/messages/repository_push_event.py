# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from marshmallow import fields

from polaris.messaging.messages import Message


class RemoteRepositoryPushEvent(Message):
    message_type = 'vcs.remote_repository_push_event'

    connector_key = fields.String(required=True)
    repository_source_id = fields.String(required=True)





