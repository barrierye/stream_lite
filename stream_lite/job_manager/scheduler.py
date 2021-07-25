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
            resp = client.requestSlot(
                    task_manager_pb2.RequiredSlotRequest(
                        slot_descs=[
                            serializator.SerializableRequiredSlotDesc.to_proto()
                            for task in seri_tasks]))
            if resp.status.err_code != 0:
                raise RuntimeError(resp.status.message)
            available_ports_map[task_manager_name] = \
                    list(resp.available_ports)
        return available_ports_map


class UserDefinedScheduler(Scheduler):

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        super(UserDefinedScheduler, self).__init__(registered_task_manager_table)

    def schedule(self, serializable_tasks: List[serializator.SerializableTask]) \
            -> Dict[str, List[serializator.SerializableExectueTask]]:
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
        for task_manager_name, seri_logical_tasks in logical_map.items():
            execute_map[task_manager_name] = []
            for idx, logical_task in enumerate(seri_logical_tasks):
                port = available_ports_map[task_manager_name][idx]
                execute_map[task_manager_name].append(
                        serializator.SerializableExectueTask(
                            cls_name=logical_task.cls_name,
                            input_endpoints=[],
                            output_endpoints=[],
                            resources=logical_task.resources,
                            task_file=logical_task.task_file,
                            port=port))

