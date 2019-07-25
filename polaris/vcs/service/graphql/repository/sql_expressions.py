# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



def repository_info_columns(repositories):
    return [
        repositories.c.url,
        repositories.c.description,
        repositories.c.integration_type,
        repositories.c.public,
        repositories.c.import_state
    ]