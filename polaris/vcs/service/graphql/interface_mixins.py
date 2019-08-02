# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.utils import create_tuple, init_tuple
from polaris.graphql.mixins import KeyIdResolverMixin
from .interfaces import RepositoryInfo, SyncStateSummary


class RepositoryInfoResolverMixin(KeyIdResolverMixin):
    repository_info_tuple = create_tuple(RepositoryInfo)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository_info = init_tuple(self.repository_info_tuple, **kwargs)

    def resolve_repository_info_interface(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['RepositoryInfo'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_repository_info(self, info, **kwargs):
        if self.repository_info is None:
            self.repository_info = self.resolve_repository_info_interface(info, **kwargs)
        return self.repository_info

    def resolve_url(self, info, **kwargs):
        return self.get_repository_info(info, **kwargs).url

    def resolve_description(self, info, **kwargs):
        return self.get_repository_info(info, **kwargs).description

    def resolve_integration_type(self, info, **kwargs):
        return self.get_repository_info(info, **kwargs).integration_type

    def resolve_import_state(self, info, **kwargs):
        return self.get_repository_info(info, **kwargs).import_state

    def resolve_public(self, info, **kwargs):
        return self.get_repository_info(info, **kwargs).public


class SyncStateSummaryResolverMixin(KeyIdResolverMixin):
    sync_state_summary_tuple = create_tuple(SyncStateSummary)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sync_state_summary = init_tuple(self.sync_state_summary_tuple, **kwargs)

    def resolve_sync_state_summary_interface(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['SyncStateSummary'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_sync_state_summary(self, info, **kwargs):
        if self.sync_state_summary is None:
            self.sync_state_summary = self.resolve_sync_state_summary_interface(info, **kwargs)
        return self.sync_state_summary

    def get_commits_in_process(self, info, **kwargs):
        return self.get_sync_state_summary(info, **kwargs).commits_in_process
