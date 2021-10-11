#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable


class StrategyBase(object):

    @staticmethod
    def get_migrate_infos(
            sub_task_table: SubTaskTable,
            latency_table: PeerLatencyTable) -> List[common_pb2.MigrateInfo]:
        """
        source 和 sink 所在的TaskManager不能变更
        """
        raise NotImplementedError()
