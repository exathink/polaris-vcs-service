# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from setuptools import setup
from os import path



import polaris.vcs.service

here = path.abspath(path.dirname(__file__))


setup(
    # --------------------------------------------------------------------------------


    name='polaris-vcs-service',

    # -------------------------------------------------------------------------------


    version=polaris.vcs.service.__version__,

    # -------------------------------------------------------------------------------
    # UNCOMMENT THE 'packages' line and define the Python 3 namespace packages for this package.
    # This should specify a package for each prefix of your package name.

    packages=[
        'polaris',
        'polaris.vcs',
        'polaris.vcs.service',
        'polaris.vcs.service.graphql',
        'polaris.vcs.service.graphql.repository',
        'polaris.vcs.service.graphql.vcs_connector',
        'polaris.vcs.messaging',
        'polaris.vcs.messaging.messages',
        'polaris.vcs.messaging.subscribers',
        'polaris.vcs.messaging.tasks',
        'polaris.vcs.db',
        'polaris.vcs.db.impl',
        'polaris.vcs.integrations',
        'polaris.vcs.integrations.atlassian',
        'polaris.vcs.integrations.gitlab',
        'polaris.vcs.integrations.github'

    ],
    url='',
    license = 'Commercial',
    author='Krishna Kumar',
    author_email='kkumar@exathink.com',
    description='',
    long_description='',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    # Run time dependencies - we will assume pytest is dependency of all packages.
    install_requires=[
        'pytest'
    ]
)
