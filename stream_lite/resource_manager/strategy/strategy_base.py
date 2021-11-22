#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
from typing import Optional, Tuple, List, Dict

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable

from ..execute_task_table import ExecuteTaskInfo


class StrategyBase(object):

    @staticmethod
    def get_migrate_infos(
            exec_task_infos: Dict[str, ExecuteTaskInfo],
            latency_table: PeerLatencyTable) -> List[common_pb2.MigrateInfo]:
        """
        source 和 sink 所在的TaskManager不能变更
        """
        raise NotImplementedError()
