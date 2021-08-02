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

    def trigger_checkpoint(self, 
            jobid: str,
            checkpoint_id: int,
            cancel_job: bool):
        with self.rw_lock_pair.gen_rlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger checkpoint: can not found job(jobid={})".format(jobid))
            self.table[jobid].trigger_checkpoint(
                    checkpoint_id, 
                    self.registered_task_manager_table,
                    cancel_job)

    def acknowledgeCheckpoint(self, 
            request: job_manager_pb2.AcknowledgeCheckpointRequest) -> bool:
        jobid = request.jobid
        with self.rw_lock_pair.gen_rlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger checkpoint: can not found job(jobid={})".format(jobid))
            return self.table[jobid].acknowledgeCheckpoint(request)


class SpecificJobInfo(object):
    """
    管理某个具体的 Job 信息
    """

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.source_ops = self.find_source_ops(execute_task_map)
        self.ack_map = AcknowledgeTable(execute_task_map)

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

    def acknowledgeCheckpoint(self, 
            request: job_manager_pb2.AcknowledgeCheckpointRequest) -> bool:
        return self.ack_map.acknowledgeCheckpoint(request)

    def trigger_checkpoint(self, 
            checkpoint_id: int,
            registered_task_manager_table: RegisteredTaskManagerTable,
            cancel_job: bool):
        """
        传入 registered_task_manager_table 是为了找到对应 task_manager 的 endpoint
        """
        if self.ack_map.has_checkpoint(checkpoint_id):
            raise KeyError(
                    "Failed: checkpoint(id={}) already exists".format(jobid))
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    registered_task_manager_table.get_task_manager_ip(
                            task_manager_name)
            for task in tasks:
                self._inner_trigger_checkpoint(
                        task_manager_ip, 
                        task.port, 
                        task.subtask_name,
                        checkpoint_id,
                        cancel_job)
        self.ack_map.register_pending_checkpoint(checkpoint_id)

    def _inner_trigger_checkpoint(self, 
            subtask_ip: str, 
            subtask_port: int, 
            subtask_name: str,
            checkpoint_id: int,
            cancel_job: bool):
        subtask_endpoint = "{}:{}".format(subtask_ip, subtask_port)
        client = SubTaskClient()
        client.connect(subtask_endpoint)
        _LOGGER.info(
                "Try to trigger checkpoint(id={}) for subtask [{}] (endpoint={})"
                .format(checkpoint_id, subtask_name, subtask_endpoint))
        client.triggerCheckpoint(checkpoint_id, cancel_job)


class AcknowledgeTable(object):
    """
    checkpoint_id -> pending_checkpoint
    """

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.execute_task_map = execute_task_map
        self.pending_checkpoints = {}

    def has_checkpoint(self, checkpoint_id: int):
        return checkpoint_id in self.pending_checkpoints

    def register_pending_checkpoint(checkpoint_id: int):
        if checkpoint_id in self.pending_checkpoint:
            raise KeyError(
                    "Failed: checkpoint(id={}) already exists".format(checkpoint_id))
        self.pending_checkpoints[checkpoint_id] = set()
        for task_manager_name, tasks in self.execute_task.items():
            for task in tasks:
                subtask_name = task.subtask_name
                self.pending_checkpoints[checkpoint_id].add(subtask_name)

    def acknowledgeCheckpoint(self,
            request: job_manager_pb2.AcknowledgeCheckpointRequest) -> bool:
        """
        返回是否完全 ack
        """
        checkpoint_id = request.checkpoint_id
        if checkpoint_id not in self.pending_checkpoint:
            raise KeyError(
                    "Failed: checkpoint(id={}) not exists".format(checkpoint_id))
        subtask_name = request.subtask_name
        pending_checkpoint = self.pending_checkpoints[checkpoint_id]
        if subtask_name not in pending_checkpoint:
            _LOGGER.warning("{} already acknowledge".format(subtask_name))
        else:
            pending_checkpoint.remove(subtask_name)
        if len(pending_checkpoint) == 0:
            self.pending_checkpoints.pop(checkpoint_id)
            return True
        return False
