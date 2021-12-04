#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-01
from typing import List, Dict, Tuple, Union
from readerwriterlock import rwlock
import logging
import threading

from stream_lite.proto import common_pb2
from stream_lite.proto import subtask_pb2
from stream_lite.proto import job_manager_pb2

from stream_lite.client import SubTaskClient, ResourceManagerClient
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class JobCoordinator(object):

    def __init__(self, resource_manager_client: ResourceManagerClient):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.resource_manager_client = resource_manager_client
        self.table = {} # jobid -> SpecificJobInfo

    def register_job(self, 
            jobid: str, 
            execute_task_map: Dict[str, List[serializator.SerializableExectueTask]]):
        with self.rw_lock_pair.gen_wlock():
            if jobid in self.table:
                raise KeyError(
                        "Failed to register job: jobid({}) already exists".format(jobid))
            if len(self.table) > 0:
                raise RuntimeError(
                        "Failed to register job: here only supports one job now.")
            self.table[jobid] = SpecificJobInfo(execute_task_map)
            #  _LOGGER.debug("Registering job: jobid({})".format(jobid))

    def trigger_checkpoint(self, 
            jobid: str,
            checkpoint_id: int,
            cancel_job: bool,
            new_streaming_name: str,
            migrate_cls_name: str = "",
            migrate_partition_idx: int = -1) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger checkpoint: can not found job(jobid={})".format(jobid))
            self.table[jobid].trigger_checkpoint(
                    checkpoint_id, 
                    self.resource_manager_client,
                    cancel_job,
                    migrate_cls_name,
                    migrate_partition_idx,
                    new_streaming_name)

    def trigger_checkpoint_for_migrate(self, 
            jobid: str,
            checkpoint_id: int,
            migrate_cls_name: List[str],
            migrate_partition_idx: List[List[int]]) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger checkpoint: can not found job(jobid={})".format(jobid))
            self.table[jobid].trigger_checkpoint_for_migrate(
                    checkpoint_id, 
                    self.resource_manager_client,
                    migrate_cls_name,
                    migrate_partition_idx)

    def trigger_migrate(self, 
            jobid: str,
            new_cls_name: str,
            new_partition_idx: int,
            new_endpoint: str,
            migrate_id: int,
            register: bool = True) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to trigger migrate: can not found job(jobid={})".format(jobid))
            self.table[jobid].trigger_migrate(
                    migrate_id,
                    self.resource_manager_client,
                    new_cls_name,
                    new_partition_idx,
                    new_endpoint,
                    register)

    def register_pending_migrate_sync(self,
            jobid: str,
            migrate_id: int) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to register pending_migrate_sync: can not" +\
                        " found job(jobid={})".format(jobid))
            self.table[jobid].register_pending_migrate_sync(migrate_id)

    def terminate_subtask(self,
            jobid: str,
            cls_name: str,
            partition_idx: int,
            subtask_name: str,
            terminate_id: int) -> None:
        """
        通过类似 checkpoint 的机制，给 source 发 event
        """
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to terminate subtask: can" +\
                        " not found job(jobid={})".format(jobid))
            self.table[jobid].terminate_subtask(
                    terminate_id,
                    self.resource_manager_client,
                    cls_name,
                    partition_idx,
                    subtask_name)

    def acknowledgeCheckpoint(self, 
            request: job_manager_pb2.AcknowledgeCheckpointRequest) -> bool:
        jobid = request.jobid
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to ack checkpoint: can not found job(jobid={})".format(jobid))
            return self.table[jobid].acknowledgeCheckpoint(request)

    def acknowledgeMigrate(self, 
            request: job_manager_pb2.AcknowledgeMigrateRequest) -> bool:
        jobid = request.jobid
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to ack migrate: can can not found job(jobid={})".format(jobid))
            return self.table[jobid].acknowledgeMigrate(request)

    def acknowledgeTerminate(self, 
            request: job_manager_pb2.AcknowledgeTerminateRequest) -> bool:
        jobid = request.jobid
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to ack terminate: can can not found job(jobid={})".format(jobid))
            return self.table[jobid].acknowledgeTerminate(request)

    def notifyMigrateSynchron(self, 
            request: job_manager_pb2.NotifyMigrateSynchronRequest) -> bool:
        jobid = request.jobid
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to notify migrate sync: can can not found job(jobid={})".format(jobid))
        return self.table[jobid].notifyMigrateSynchron(request)

    def block_util_checkpoint_completed(self,
            jobid: str,
            checkpoint_id: int) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to block checkpoint: can not found job(jobid={})".format(jobid))
        self.table[jobid].block_util_event_completed(checkpoint_id)

    def block_util_migrate_completed(self,
            jobid: str,
            migrate_id: int) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to block migrate: can not found job(jobid={})".format(jobid))
        self.table[jobid].block_util_event_completed(migrate_id)

    def block_util_terminate_completed(self,
            jobid: str,
            terminate_id: int) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to block terminate: can not found job(jobid={})".format(jobid))
        self.table[jobid].block_util_event_completed(terminate_id)

    def block_util_migrate_sync(self,
            jobid: str,
            migrate_id: int) -> None:
        with self.rw_lock_pair.gen_wlock():
            if jobid not in self.table:
                raise KeyError(
                        "Failed to block migrate: can not found job(jobid={})".format(jobid))
        self.table[jobid].block_util_migrate_sync(migrate_id)
        


class SpecificJobInfo(object):
    """
    管理某个具体的 Job 信息
    """

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.source_ops = self.find_source_ops(execute_task_map)
        self.ack_table = EventAcknowledgeTable(execute_task_map)

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
        return self.ack_table.acknowledgeCheckpoint(request)

    def acknowledgeMigrate(self, 
            request: job_manager_pb2.AcknowledgeMigrateRequest) -> bool:
        return self.ack_table.acknowledgeMigrate(request)

    def acknowledgeTerminate(self, 
            request: job_manager_pb2.AcknowledgeTerminateRequest) -> bool:
        return self.ack_table.acknowledgeTerminate(request)

    def block_util_event_completed(self, event_id: int) -> None:
        if not self.ack_table.has_event(event_id):
            raise KeyError(
                    "Failed: event(id={}) not exists".format(event_id))
        self.ack_table.block_util_event_completed(event_id)

    def trigger_checkpoint(self, 
            checkpoint_id: int,
            resource_manager_client: ResourceManagerClient,
            cancel_job: bool,
            migrate_cls_name: str,
            migrate_partition_idx: int,
            new_streaming_name: str) -> None:
        if self.ack_table.has_event(checkpoint_id):
            raise KeyError(
                    "Failed: checkpoint(id={}) already exists".format(checkpoint_id))
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    resource_manager_client.get_host(task_manager_name)
            for task in tasks:
                self._inner_trigger_checkpoint(
                        subtask_ip=task_manager_ip, 
                        subtask_port=task.port, 
                        subtask_name=task.subtask_name,
                        checkpoint_id=checkpoint_id,
                        cancel_job=cancel_job,
                        new_streaming_name=new_streaming_name,
                        migrate_cls_name=migrate_cls_name,
                        migrate_partition_idx=migrate_partition_idx,
                        trigger_checkpoint_for_migrate=False)
        self.ack_table.register_pending_event(checkpoint_id)

    def trigger_checkpoint_for_migrate(self,
            checkpoint_id: int,
            resource_manager_client: ResourceManagerClient,
            migrate_cls_name: List[str],
            migrate_partition_idx: List[List[int]]) -> None:
        """
        传入 resource_manager_client 是为了找到对应 task_manager 的 endpoint
        """
        if self.ack_table.has_event(checkpoint_id):
            raise KeyError(
                    "Failed: checkpoint(id={}) already exists".format(checkpoint_id))
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    resource_manager_client.get_host(task_manager_name)
            for task in tasks:
                self._inner_trigger_checkpoint(
                        subtask_ip=task_manager_ip, 
                        subtask_port=task.port, 
                        subtask_name=task.subtask_name,
                        checkpoint_id=checkpoint_id,
                        migrate_cls_name=migrate_cls_name,
                        migrate_partition_idx=migrate_partition_idx,
                        trigger_checkpoint_for_migrate=True)
        self.ack_table.register_pending_event(checkpoint_id)

    def _inner_trigger_checkpoint(self, 
            subtask_ip: str, 
            subtask_port: int, 
            subtask_name: str,
            checkpoint_id: int,
            trigger_checkpoint_for_migrate: bool,
            cancel_job: bool = False,
            new_streaming_name: str = "",
            migrate_cls_name: Union[None, List[str]] = None,
            migrate_partition_idx: Union[None, List[List[int]]] = None) -> None:
        subtask_endpoint = "{}:{}".format(subtask_ip, subtask_port)
        client = SubTaskClient()
        client.connect(subtask_endpoint)
        if not trigger_checkpoint_for_migrate:
            # checkpoint
            _LOGGER.info(
                    "Try to trigger checkpoint(id={}) for subtask [{}] (endpoint={})"
                    .format(checkpoint_id, subtask_name, subtask_endpoint))
            client.triggerCheckpoint(checkpoint_id, cancel_job,
                    migrate_cls_name, migrate_partition_idx, new_streaming_name)
        else:
            # checkpoint prepare for migrate
            _LOGGER.info(
                    "Try to trigger checkpoint for migrate (id={}) for subtask [{}] (endpoint={})"
                    .format(checkpoint_id, subtask_name, subtask_endpoint))
            client.triggerCheckpointPrepareForMigrate(
                    checkpoint_id, migrate_cls_name, migrate_partition_idx, new_streaming_name)

    def trigger_migrate(self, 
            migrate_id: int,
            resource_manager_client: ResourceManagerClient,
            new_cls_name: str,
            new_partition_idx: int,
            new_endpoint: str,
            register: bool = True) -> None:
        if register and self.ack_table.has_event(migrate_id):
            raise KeyError(
                    "Failed: migrate(id={}) already exists".format(migrate_id))
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    resource_manager_client.get_host(task_manager_name)
            for task in tasks:
                self._inner_trigger_migrate(
                        task_manager_ip,
                        task.port,
                        task.subtask_name,
                        migrate_id,
                        new_cls_name,
                        new_partition_idx,
                        new_endpoint)
        if register:
            self.ack_table.register_pending_event(migrate_id)

    def _inner_trigger_migrate(self,
            subtask_ip: str,
            subtask_port: int,
            subtask_name: str,
            migrate_id: int,
            new_cls_name: str,
            new_partition_idx: int,
            new_endpoint: str) -> None:
        subtask_endpoint = "{}:{}".format(subtask_ip, subtask_port)
        client = SubTaskClient()
        client.connect(subtask_endpoint)
        _LOGGER.info(
                "Try to trigger migrate(id={}) for subtask [{}] (endpoint={})"
                .format(migrate_id, subtask_name, subtask_endpoint))
        client.triggerMigrate(migrate_id, new_cls_name, new_partition_idx, new_endpoint)

    def terminate_subtask(self,
            terminate_id: int,
            resource_manager_client: ResourceManagerClient,
            cls_name: str,
            partition_idx: int,
            subtask_name: str) -> None:
        if self.ack_table.has_event(terminate_id):
            raise KeyError(
                    "Failed: terminate(id={}) already exists".format(terminate_id))
        for task_manager_name, tasks in self.source_ops.items():
            task_manager_ip = \
                    resource_manager_client.get_host(task_manager_name)
            for task in tasks:
                self._inner_terminate_subtask(
                        task_manager_ip,
                        task.port,
                        task.subtask_name,
                        terminate_id,
                        cls_name, 
                        partition_idx, 
                        subtask_name)
        self.ack_table.register_pending_event(terminate_id)

    def _inner_terminate_subtask(self,
            subtask_ip: str,
            subtask_port: int,
            subtask_name: str,
            terminate_id: int,
            terminate_cls_name: str,
            terminate_partition_idx: int,
            terminate_subtask_name: str) -> None:
        subtask_endpoint = "{}:{}".format(subtask_ip, subtask_port)
        client = SubTaskClient()
        client.connect(subtask_endpoint)
        _LOGGER.info(
                "Try to trigger terminate(id={}) for subtask [{}] (endpoint={})"
                .format(terminate_id, subtask_name, subtask_endpoint))
        client.terminateSubtask(terminate_id, terminate_cls_name,
                terminate_partition_idx, terminate_subtask_name)

    def register_pending_migrate_sync(self, migrate_id: int) -> None:
        if self.ack_table.has_event(migrate_id):
            raise KeyError(
                    "Failed: migrate(id={}) already exists".format(migrate_id))
        self.ack_table.register_pending_migrate_sync(migrate_id)

    def notifyMigrateSynchron(self, 
            request: job_manager_pb2.NotifyMigrateSynchronRequest) -> bool:
        return self.ack_table.notifyMigrateSynchron(request)

    def block_util_migrate_sync(self, event_id: int) -> None:
        if not self.ack_table.has_event(event_id):
            raise KeyError(
                    "Failed: event(id={}) not exists".format(event_id))
        self.ack_table.block_util_migrate_sync(event_id)


class EventAcknowledgeTable(object):
    """
    event_id -> pending_event
    """

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.execute_task_map = execute_task_map
        self.pending_events = {} # eventid -> pending_event

    def has_event(self, event_id: int):
        return event_id in self.pending_events

    def register_pending_event(self, event_id: int):
        if event_id in self.pending_events:
            raise KeyError(
                    "Failed: event(id={}) already exists".format(event_id))
        self.pending_events[event_id] = PendingEvent(self.execute_task_map)

    def register_pending_migrate_sync(self, migrate_id: int) -> None:
        if migrate_id in self.pending_events:
            raise KeyError(
                    "Failed: event(id={}) already exists".format(migrate_id))
        self.pending_events[migrate_id] = PendingForNotify()

    def acknowledgeCheckpoint(self,
            request: job_manager_pb2.AcknowledgeCheckpointRequest) -> bool:
        """
        返回是否完全 ack
        """
        checkpoint_id = request.checkpoint_id
        if checkpoint_id not in self.pending_events:
            raise KeyError(
                    "Failed: checkpoint(id={}) not exists".format(checkpoint_id))
        subtask_name = request.subtask_name
        return self.pending_events[checkpoint_id].acknowledgeEvent(subtask_name)

    def acknowledgeMigrate(self,
            request: job_manager_pb2.AcknowledgeMigrateRequest) -> bool:
        migrate_id = request.migrate_id
        if migrate_id not in self.pending_events:
            raise KeyError(
                    "Failed: migrate(id={}) not exists".format(migrate_id))
        subtask_name = request.subtask_name
        return self.pending_events[migrate_id].acknowledgeEvent(subtask_name)

    def acknowledgeTerminate(self,
            request: job_manager_pb2.AcknowledgeTerminateRequest) -> bool:
        terminate_id = request.terminate_id
        if terminate_id not in self.pending_events:
            raise KeyError(
                    "Failed: terminate(id={}) not exists".format(terminate_id))
        subtask_name = request.subtask_name
        return self.pending_events[terminate_id].acknowledgeEvent(subtask_name)

    def notifyMigrateSynchron(self,
            request: job_manager_pb2.NotifyMigrateSynchronRequest) -> None:
        migrate_id = request.migrate_id
        if migrate_id not in self.pending_events:
            raise KeyError(
                    "Failed: migrate(id={}) not exists".format(migrate_id))
        self.pending_events[migrate_id].notify()

    def block_util_event_completed(self, event_id: int) -> None:
        if event_id not in self.pending_events:
            raise KeyError(
                    "Failed: event(id={}) not exists".format(event_id))
        self.pending_events[event_id].block_util_event_completed()
        self.pending_events.pop(event_id)

    def block_util_migrate_sync(self, migrate_id: int) -> None:
        if migrate_id not in self.pending_events:
            raise KeyError(
                    "Failed: event(id={}) not exists".format(migrate_id))
        self.pending_events[migrate_id].block()
        self.pending_events.pop(migrate_id)


class PendingEvent(object):

    def __init__(self, 
            execute_task_map: Dict[str, List[serializator.SerializableTask]]):
        self.pending_subtasks = set()
        for task_manager_name, tasks in execute_task_map.items():
            for task in tasks:
                subtask_name = task.subtask_name
                self.pending_subtasks.add(subtask_name)
        self.cv = threading.Condition()  # for block event

    def acknowledgeEvent(self, subtask_name: str) -> bool:
        """
        返回是否完全 ack
        """
        if subtask_name not in self.pending_subtasks:
            _LOGGER.warning("{} already acknowledge".format(subtask_name))
        else:
            with self.cv:
                self.pending_subtasks.remove(subtask_name)
                if self.is_event_completed():
                    self.cv.notify_all()
                    _LOGGER.info("finish to ack!")
                    return True
                self.cv.notify_all()
        return False

    def is_event_completed(self) -> None:
        """ not thread safe """
        return len(self.pending_subtasks) == 0

    def block_util_event_completed(self) -> None:
        with self.cv:
            while not self.is_event_completed():
                self.cv.wait()


class PendingForNotify(object):

    def __init__(self):
        self.cv = threading.Condition()  # for block

    def notify(self) -> None:
        with self.cv:
            self.cv.notify_all()

    def block(self) -> None:
        with self.cv:
            self.cv.wait()
