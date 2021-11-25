#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import copy
from typing import List, Dict, Tuple
import logging

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.resource_manager.subtask_table import SubTaskTable, SubTaskDesc
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable
from stream_lite.resource_manager.execute_task_table import ExecuteTaskInfo

from .strategy_base import StrategyBase


_LOGGER = logging.getLogger(__name__)

class GreedyStrategy(StrategyBase):

    @staticmethod
    def get_migrate_infos(
            jobid: str,
            exec_task_infos: Dict[str, ExecuteTaskInfo],
            latency_table: PeerLatencyTable) -> List[common_pb2.MigrateInfo]:
        # 0. 找所有 source 和 sink
        sources = []
        sinks = []
        for subtask_name, exec_task_info in exec_task_infos.items():
            if exec_task_info.is_source():
                sources.append(subtask_name)
            if exec_task_info.is_sink():
                sinks.append(subtask_name)

        migrate_info_list = []

        # sink在一个云节点
        dst_info = exec_task_infos[sinks[0]]
        dst_node_name = dst_info.task_manager_name
        assert len(sinks) == 1

        # 可能有多个source，故有多条链式图
        for source_subtask_name in sources:
            # 1. 图缩点
            chain_graph = []
            que = [source_subtask_name]
            total_latency = 0
            while True:
                maxL = -1.0
                newQue = []
                curr_set = set()
                for curr_subtask_name in que:
                    curr_info = exec_task_infos[curr_subtask_name]
                    curr_set.add(curr_subtask_name)
                    curr_task_manager_name = curr_info.task_manager_name
                    for next_subtask_name in curr_info.downstream_cls_names:
                        newQue.append(next_subtask_name)
                        next_info = exec_task_infos[next_subtask_name]
                        next_task_manager_name = next_info.task_manager_name
                        latency = latency_table.get_latency(
                                curr_task_manager_name, next_task_manager_name)
                        maxL = max(maxL, latency)
                if len(newQue) == 0:
                    break
                if maxL <= 0 and len(chain_graph) >= 1:
                    # 在同一台机器上
                    last = chain_graph.pop(-1)
                    last_set, last_maxL = last
                    chain_graph.append((last_set | curr_set, last_maxL))
                else:
                    total_latency += maxL
                    chain_graph.append((curr_set, maxL))
                que = newQue

            # 2. 最短路
            # 可能有多个source，故找多个最短路
            src_info = exec_task_infos[source_subtask_name]
            src_node_name = src_info.task_manager_name
            path, shortest_latency = GreedyStrategy._dijkstra(src_node_name, dst_node_name, latency_table)
            print(">>> path: {}".format(path))
            if total_latency > shortest_latency:
                _LOGGER.info(
                    "current latency({}) > shortest latency({}), try to gen migrate info...".format(
                        total_latency, shortest_latency))
            # 匹配一下
            if len(chain_graph) < len(path):
                return []
            else:
                for idx, chain_node in enumerate(chain_graph):
                    subtask_name_set = chain_node[0]
                    for subtask_name in subtask_name_set:
                        m_src_info = exec_task_infos[subtask_name]
                        target_task_manager_name = path[min(idx, len(path) - 1)]
                        if m_src_info.task_manager_name != target_task_manager_name:
                            migrate_info_list.append(
                                    common_pb2.MigrateInfo(
                                        src_cls_name=m_src_info.cls_name,
                                        target_task_manager_locate=target_task_manager_name,
                                        jobid=jobid,
                                        src_currency=m_src_info.currency,
                                        src_partition_idx=m_src_info.partition_idx))
        return migrate_info_list

    @staticmethod
    def _dijkstra(src: str, dst: str, latency_table: PeerLatencyTable) \
            -> Tuple[List[str], float]:
        # 返回path数组以及最短路
        class Edge:
            v = 0
            w = 0
        
        # init edges
        stable_latency_table = latency_table.get_latency()
        e = {} # from: [(to, latency)]
        for from_e, latency_list in stable_latency_table.items():
            if from_e not in e:
                e[from_e] = []
            for to_e, latency in latency_list.items():
                e[from_e].append((to_e, latency))
                if to_e not in e:
                    e[to_e] = []
                e[to_e].append((from_e, latency))
        
        # init dis & vis & pre
        node_n = len(stable_latency_table)
        dis = {name: float('inf') for name in e}
        vis = {name: False for name in e}
        pre = {name: None for name in e}

        # dijkstra
        dis[src] = 0
        for _ in range(node_n):
            u = None
            mind = float('inf')
            for cur_name in stable_latency_table:
                if vis[cur_name] == False and dis[cur_name] < mind:
                    u = cur_name
                    mind = dis[cur_name]
            vis[u] = True
            for nxt in e[cur_name]:
                nxt_name, latency = nxt
                if dis[nxt_name] > dis[cur_name] + latency:
                    dis[nxt_name] = dis[cur_name] + latency
                    pre[nxt_name] = cur_name
        
        # get path
        path = []
        last = dst
        while last != None:
            path.append(last)
            last = pre[last]
        path.reverse()

        return (path, dis[dst])
