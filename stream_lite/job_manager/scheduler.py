#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging
from typing import List, Dict

from .registered_task_manager_table import RegisteredTaskManagerTable
from stream_lite.proto import task_manager_pb2
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class Scheduler(object):

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        self.registered_task_manager_table = registered_task_manager_table

    def schedule(self, serializable_tasks: List[serializator.SerializableTask]) \
            -> Dict[str, List[serializator.SerializableTask]]:
        raise NotImplementedError("Failed: function not implemented")
 
    def transform_logical_map_to_execute_map(self, 
            logical_map: Dict[str, List[serializator.SerializableTask]]) \
                    -> Dict[str, List[serializator.SerializableExectueTask]]:
        raise NotImplementedError("Failed: function not implemented")

    def ask_for_available_ports(self, 
            logical_map: Dict[str, List[serializator.SerializableTask]]) \
                    -> Dict[str, List[int]]:
        available_ports_map = {} # taskmanager.name -> List[int]
        for task_manager_name, seri_tasks in logical_map.items():
            client = self.registered_task_manager_table.get_client(task_manager_name)
            slot_desc_protos = []
            for task in seri_tasks:
                slot_desc_protos.extend(
                        [serializator.SerializableRequiredSlotDesc.to_proto()
                            for i in range(task.currency)])
            resp = client.requestSlot(
                    task_manager_pb2.RequiredSlotRequest(
                        slot_descs=slot_desc_protos))
            if resp.status.err_code != 0:
                raise RuntimeError(resp.status.message)
            available_ports_map[task_manager_name] = \
                    list(resp.available_ports)
        return available_ports_map

    def _get_subtask_name(self, cls_name: str, idx: int, currency: int) -> str:
        return "{}#({}/{})".format(cls_name, idx, currency)


class UserDefinedScheduler(Scheduler):
    """
    用户定义的 Scheduler
    只能定义不同 Task 在不同的 TaskManager 上，不能定义同个 Task 的不同 subTask
    """

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        super(UserDefinedScheduler, self).__init__(registered_task_manager_table)

    def schedule(self, serializable_tasks: List[serializator.SerializableTask]) \
            -> Dict[str, List[serializator.SerializableExectueTask]]:
        """
        logical_map: 每个 TaskManager 里有哪些 logical_task (with currency)
        execute_map: 每个 TaskManager 里有哪些 execute_task (subTask)
        """
        logical_map = {} # taskmanager.name -> List[serializable_task]
        for task in serializable_tasks:
            name = task.locate
            if not self.registered_task_manager_table.hasTaskManager(name):
                raise RuntimeError(
                        "Failed: task.locate({}) not registerd.".format(name))
            if name not in logical_map:
                logical_map[name] = []
            logical_map[name].append(task)
        execute_map = self.transform_logical_map_to_execute_map(logical_map)
        return execute_map

    def transform_logical_map_to_execute_map(self,
            logical_map: Dict[str, List[serializator.SerializableTask]]) \
                    -> Dict[str, List[serializator.SerializableExectueTask]]:
        available_ports_map = self.ask_for_available_ports(logical_map)
        execute_map = {} # taskmanager.name -> List[serializable_execute_task]
        name_to_executetask = {} # subtask_name -> serializable_execute_task
        for task_manager_name, seri_logical_tasks in logical_map.items():
            task_manager_host = self.registered_task_manager_table.get_host(task_manager_name)
            execute_map[task_manager_name] = []
            used_port_idx = 0
            for logical_task in seri_logical_tasks:
                for i in range(logical_task.currency):
                    port = available_ports_map[task_manager_name][used_port_idx]
                    used_port_idx += 1
                    subtask_name = self._get_subtask_name(
                            logical_task.cls_name, i, 
                            logical_task.currency)
                    execute_task = serializator.SerializableExectueTask(
                                cls_name=logical_task.cls_name,
                                input_endpoints=[],
                                output_endpoints=[],
                                resources=logical_task.resources,
                                task_file=logical_task.task_file,
                                port=port,
                                host=task_manager_host,
                                subtask_name=subtask_name,
                                partition_idx=i)
                    execute_map[task_manager_name].append(execute_task)
                    name_to_executetask[subtask_name] = execute_task
    
        for logical_tasks in logical_map.values():
            for logical_task in logical_tasks:
                cls_name = logical_task.cls_name
                # TODO
                assert(len(logical_task.input_tasks) == 1)
                for input_task_name in logical_task.input_tasks:
                    # 找前继节点: 获取并发数
                    predecessor = None
                    for task_i in logical_tasks:
                        if task_i.cls_name == input_task_name:
                            predecessor = task_i
                            break
                    if predecessor is None:
                        raise Exception(
                                "Failed: the predecessor task(name={})".format(input_task_name) +\
                                " of task(name={}) is not found".format(cls_name))
                    # 设置 input_endpoints & output_endpoints
                    for i in range(predecessor.currency):
                        pre_subtask_name = self._get_subtask_name(
                                predecessor.cls_name, i, predecessor.currency)
                        pre_executetask = name_to_executetask[pre_subtask_name]
                        for j in range(logical_task.currency):
                            current_subtask_name = self._get_subtask_name(
                                    logical_task.cls_name, j, logical_task.currency)
                            current_executetask = name_to_executetask[current_subtask_name]
                            current_executetask.input_endpoints.append(
                                    "{}:{}".format(
                                        pre_executetask.host,
                                        pre_executetask.port))
                            pre_executetask.output_endpoints.append(
                                    "{}:{}".format(
                                        current_executetask.host,
                                        current_executetask.port))
        return execute_map
