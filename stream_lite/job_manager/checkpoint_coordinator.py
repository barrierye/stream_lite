#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-01
from typing import List, Dict, Tuple
from readerwriterlock import rwlock
import logging

from stream_lite.proto import common_pb2
from stream_lite.proto import subtask_pb2

from stream_lite.client import SubTaskClient
from .registered_task_manager_table import RegisteredTaskManagerTable
from stream_lite.utils import CheckpointIdGenerator
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class CheckpointCoordinator(object):

    def __init__(self, registered_task_manager_table: RegisteredTaskManagerTable):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.registered_task_manager_table = registered_task_manager_table
        self.table = {} # jobid -> SpecificJobInfo

    def register_job(self, 
            jobid: str, 
            execute_task_map: Dict[str, List[serializator.SerializableExectueTask]]):
        with self.rw_lock_pair.gen_wlock():
            if jobid in self.table:
                raise KeyError(
                        "Failed to register job: jobid({}) already exists".format(jobid))
            self.table[jobid] = SpecificJobInfo(execute_task_map)
            #  _LOGGER.debug("Registering job: jobid({})".format(jobid))

    def trigger_checkpoint(self, jobid: str, checkpoint_id: int):
        with self.rw_lock_pair.gen_rlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger checkpoint: can not found job(jobid={})".format(jobid))
            self.table[jobid].trigger_checkpoint(
                    checkpoint_id, self.registered_task_manager_table)


class SpecificJobInfo(object):
    """
    管理某个具体的 Job 信息
    """

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.source_ops = self.find_source_ops(execute_task_map)

    def find_source_ops(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]) \
            -> Dict[str, List[serializator.SerializableExectueTask]]:
        source_ops = {}
        for task_manager_name, tasks in execute_task_map.items():
            for task in tasks:
                if len(task.input_endpoints) == 0:
                    if task_manager_name not in source_ops:
                        source_ops[task_manager_name] = []
                    source_ops[task_manager_name].append(task)
        return source_ops

    def trigger_checkpoint(self, checkpoint_id: int,
            registered_task_manager_table: RegisteredTaskManagerTable):
        """
        传入 registered_task_manager_table 是为了找到对应 task_manager 的 endpoint
        """
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    registered_task_manager_table.get_task_manager_ip(
                            task_manager_name)
            for task in tasks:
                self._inner_trigger_checkpoint(
                        task_manager_ip, 
                        task.port, 
                        task.subtask_name,
                        checkpoint_id)

    def _inner_trigger_checkpoint(self, 
            subtask_ip: str, 
            subtask_port: int, 
            subtask_name: str,
            checkpoint_id: int):
        subtask_endpoint = "{}:{}".format(subtask_ip, subtask_port)
        client = SubTaskClient()
        client.connect(subtask_endpoint)
        _LOGGER.info(
                "Try to trigger checkpoint(id={}) for subtask [{}] (endpoint={})"
                .format(checkpoint_id, subtask_name, subtask_endpoint))
        client.triggerCheckpoint(checkpoint_id)
