# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from __future__ import unicode_literals

import pytest
from polaris.common.test_support import dbtest_addoption
from polaris.common.test_support import init_db
from polaris.repos.db import model, schema
from polaris.common import db


pytest_addoption = dbtest_addoption

@pytest.fixture(scope='session', autouse=True)
def db_up(pytestconfig):
    init_db(pytestconfig)


@pytest.fixture(scope='session', autouse=True)
def setup_schema(db_up):
    model.recreate_all(db.engine())




