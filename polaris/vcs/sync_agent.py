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


class VcsAgent(Agent):

    def run(self, days, limit):
        self.loop(lambda: self.sync_pull_requests(days, limit))

    def sync_pull_requests(self, days, limit):
        logger.info("Checking for pull requests to sync")

        result = api.get_pull_requests_to_sync(days=days, limit=limit)
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
                logger.info("No pull_requests found to sync...")
                break

            # fetch next batch
            if self.exit_signal_received:
                break
            logger.info(f'Fetching next batch of {limit} pull requests')
            result = api.get_pull_requests_to_sync(before=last_updated, days=days, limit=limit)

        return True


def start(name=None, poll_interval=None, one_shot=False, days=3, limit=100):
    agent = VcsAgent(
        name=name,
        poll_interval=poll_interval,
        one_shot=one_shot
    )
    logger.info("Starting agent.")
    agent.run(days, limit)


if __name__ == '__main__':

    config_logging()
    logger.info("Connecting to database....")
    db.init()
    logger.info("Initializing messaging..")
    init_topics_to_publish(VcsTopic)

    argh.dispatch_commands([
        start
    ])