#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import copy
from typing import List, Dict

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.resource_manager.subtask_table import SubTaskTable, SubTaskDesc
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable
from stream_lite.resource_manager.execute_task_table import ExecuteTaskInfo

from .strategy_base import StrategyBase


class GreedyStrategy(StrategyBase):

    @staticmethod
    def get_migrate_infos(
            exec_task_infos: Dict[str, ExecuteTaskInfo],
            latency_table: PeerLatencyTable) -> List[common_pb2.MigrateInfo]:
        # 0. 找所有 souce
        sources = []
        for subtask_name, exec_task_info in exec_task_infos.items():
            if exec_task_info.is_source():
                sources.append(subtask_name)

        # 1. 图缩点
        chain_graph = [] # 缩点后的链式图
        que = copy.deepcopy(sources)
        while True:
            maxL = -1.0
            newQue = []
            unique_names = set()
            for curr_subtask_name in que:
                curr_info = exec_task_infos[curr_subtask_name]
                unique_names.add(curr_info.cls_name)
                curr_task_manager_name = curr_info.task_manager_name
                for next_subtask_name in curr_info.downstream_cls_names:
                    newQue.append(next_subtask_name)
                    next_info = exec_task_infos[next_subtask_name]
                    next_task_manager_name = next_info.task_manager_name
                    latency = latency_table.get_latency(
                            curr_task_manager_name, next_task_manager_name)
                    maxL = max(maxL, latency)
            chain_graph.append((unique_names, maxL))
            if len(newQue) == 0:
                break

        print(chain_graph)

        # TODO: 2. 最短路
