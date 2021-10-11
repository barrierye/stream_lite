#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import copy
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.resource_manager.subtask_table import SubTaskTable, SubTaskDesc
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable


class DynamicProgrammingStrategy(object):

    @staticmethod
    def get_migrate_infos(
            src_subtask_table: SubTaskTable,
            src_latency_table: PeerLatencyTable) -> List[common_pb2.MigrateInfo]:
        virtual_task_manager_name = "virtual_task_manager"
        subtask_table = copy.deepcopy(src_subtask_table)

        # add super souce
        source_subtaks = subtask_table.get_sources()
        super_souce = SubTaskDesc(
                name="super_souce",
                task_manager_name=virtual_task_manager_name,
                is_source=False,
                is_sink=False,
                downstream_name=[x.name for x in source_subtaks])
        subtask_table.add_subtask(super_souce)

        # add super sink
        sink_subtasks = subtask_table.get_sinks()
        super_sink = SubTaskDesc(
                name="super_sink",
                task_manager_name=virtual_task_manager_name,
                is_source=False,
                is_sink=False,
                downstream_name=[])
        for sink_subtask in sink_subtasks:
            sink_subtask.downstream_name = super_sink.name
        subtask_table.add_subtask(super_sink)

        subtasks = subtask_table.get_subtasks()
        total = len(subtasks)
        dp = [float('inf') for _ in range(total)]

        latency_table = copy.deepcopy(src_latency_table.get_latency())
        for peer_latency_table in latency_table.values():
            peer_latency_table[virtual_task_manager_name] = 0
        latency_table[virtual_task_manager_name] = \
                {key: 0 for key in latency_table.keys()}
        
