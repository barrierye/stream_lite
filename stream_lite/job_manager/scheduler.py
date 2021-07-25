#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging
from typing import List, Dict

from .registered_task_manager_table import RegisteredTaskManagerTable
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class Scheduler(object):

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        self.registered_task_manager_table = registered_task_manager_table

    def schedule(self, serializable_tasks: List[serializator.SerializableTask]) \
            -> Dict[str, serializator.SerializableTask]:
        raise NotImplementedError("Failed: function not implemented")


class UserDefinedScheduler(Scheduler):

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        super(UserDefinedScheduler, self).__init__(registered_task_manager_table)

    def schedule(self, serializable_tasks: List[serializator.SerializableTask]) \
            -> Dict[str, List[serializator.SerializableTask]]:
        schedule_map = {} # taskmanager.name -> List[serializable_task]
        for task in serializable_tasks:
            name = task.locate
            if not self.registered_task_manager_table.hasTaskManager(name):
                raise RuntimeError(
                        "Failed: task.locate({}) not registerd.".format(name))
            if name not in schedule_map:
                schedule_map[name] = []
            schedule_map[name].append(task)
        return schedule_map
