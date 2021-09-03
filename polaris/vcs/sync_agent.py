# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import argh
from polaris.utils.agent import Agent
from polaris.utils.logging import config_logging

from polaris.common import db
from polaris.vcs.db import api
from polaris.messaging.topics import VcsTopic
from polaris.messaging.utils import init_topics_to_publish, shutdown
from polaris.vcs.messaging import publish
from logging import getLogger

logger = getLogger('polaris.vcs.sync_agent')


class VcsSourceSyncAgent(Agent):

    def run(self, days, limit):
        self.loop(lambda: self.sync_pull_requests_with_source(days, limit))

    def sync_pull_requests_with_source(self, days, limit):
        logger.info("Checking for pull requests to sync with remote source")

        result = api.get_pull_requests_to_sync_with_source(days=days, limit=limit)
        last_updated = None
        while result['success']:
            if len(result['pull_requests']) > 0:
                for pull_request in result['pull_requests']:
                    publish.sync_pull_request(
                        organization_key=pull_request['organization_key'],
                        repository_key=pull_request['repository_key'],
                        pull_request_key=pull_request['pull_request_key']
                    )
                    last_updated = pull_request['source_last_updated']
                    if self.exit_signal_received:
                        shutdown()
                        break

            else:
                logger.info("No pull_requests left to sync...")
                break

            # fetch next batch
            if self.exit_signal_received:
                break
            logger.info(f'Fetching next batch of {limit} pull requests')
            result = api.get_pull_requests_to_sync_with_source(before=last_updated, days=days, limit=limit)

        return True


class VcsAnalyticsSyncAgent(Agent):

    def run(self, days, limit):
        self.loop(lambda: self.sync_pull_requests_with_analytics(days=days, limit=limit))

    def sync_pull_requests_with_analytics(self, days, limit):
        logger.info("Checking for pull requests to sync with analytics")

        result = api.get_pull_requests_to_sync_with_analytics(days=days, limit=limit)
        last_updated = None
        while result['success']:
            if len(result['pull_requests']) > 0:
                for pull_request in result['pull_requests']:
                    pull_request_summary = pull_request['pull_request_summary']
                    if pull_request_summary['is_new']:
                        publish.pull_request_created_event(
                            pull_request[
                                'organization_key'
                            ],
                            pull_request[
                                'repository_key'
                            ],
                            [
                                pull_request_summary
                            ]
                        )
                    else:
                        publish.pull_request_updated_event(
                            pull_request[
                                'organization_key'
                            ],
                            pull_request[
                                'repository_key'
                            ],
                            [
                                pull_request_summary
                            ]
                        )
                    last_updated = pull_request_summary['updated_at']
                    if self.exit_signal_received:
                        shutdown()
                        break
            else:
                logger.info('No pull requests to sync')
                break

            if self.exit_signal_received:
                break
            # get the next batch
            result = api.get_pull_requests_to_sync_with_analytics(before=last_updated, limit=limit)

        return True


# Command line drivers
# This is the default command
def start(name=None, poll_interval=None, one_shot=False, days=3, limit=100):
    agent = VcsSourceSyncAgent(
        name=name,
        poll_interval=poll_interval,
        one_shot=one_shot
    )
    logger.info("Starting agent.")
    agent.run(days, limit)


def sync_pull_requests_with_source(name=None, poll_interval=None, one_shot=False, days=3, limit=100):
    start(name, poll_interval, one_shot, days, limit)


def sync_pull_requests_with_analytics(name=None, poll_interval=None, one_shot=False, days=1, limit=100):
    agent = VcsAnalyticsSyncAgent(
        name=name,
        poll_interval=poll_interval,
        one_shot=one_shot
    )
    logger.info("Starting VcsAnalyticsSyncAgent")
    agent.run(days, limit)


if __name__ == '__main__':
    config_logging()
    logger.info("Connecting to database....")
    db.init()
    logger.info("Initializing messaging..")
    init_topics_to_publish(VcsTopic)

    argh.dispatch_commands([
        start,
        sync_pull_requests_with_source,
        sync_pull_requests_with_analytics
    ])
