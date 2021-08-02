#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import multiprocessing
from readerwriterlock import rwlock
import logging
import os

import stream_lite.proto.common_pb2 as common_pb2

import stream_lite.config
from stream_lite.network import serializator
from stream_lite.server.subtask_server import SubTaskServer

_LOGGER = logging.getLogger(__name__)


class Slot(object):

    def __init__(self, tm_name: str,
            job_manager_enpoint: str,
            execute_task: serializator.SerializableExectueTask):
        self.tm_name = tm_name
        self.job_manager_enpoint = job_manager_enpoint
        self.subtask = SubTaskServer(
                tm_name, job_manager_enpoint, execute_task)
        self.status = "DEPLOYED"

    def start(self):
        self.subtask.run_on_standalone_process(
                is_process=stream_lite.config.IS_PROCESS)

    def __str__(self):
        return "[{}] subtask_name: {}, status: {}".format(
                self.cls_name, 
                self.execute_task.subtask_name, 
                self.status)


class SlotTable(object):

    def __init__(self, tm_name: str, job_manager_enpoint: str, capacity: int):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.tm_name = tm_name
        self.capacity = capacity
        self.job_manager_enpoint = job_manager_enpoint
        self.table = {} # subtask_name -> Slot

    def deployExecuteTask(self, proto: common_pb2.ExecuteTask) -> None:
        """
        add a slot by execute_task
        """
        execute_task = serializator.SerializableExectueTask.from_proto(proto)
        with self.rw_lock_pair.gen_wlock():
            if len(self.table) >= self.capacity:
                raise RuntimeError(
                        "Failed to deploy task: out of slot_table capacity")
            subtask_name = execute_task.subtask_name
            if subtask_name in self.table:
                raise KeyError(
                        "Failed to deploy task: subtask_name({}) already exists"
                        .format(subtask_name))
            self.table[subtask_name] = Slot(
                    self.tm_name, self.job_manager_enpoint, execute_task)
            _LOGGER.debug("Succ deploy task: {}".format(subtask_name))

    def startExecuteTask(self, subtask_name: str) -> None:
        slot = self.getSlot(subtask_name)
        slot.start()
        _LOGGER.debug("Succ start task: {}".format(subtask_name))

    def hasSlot(self, name: str) -> bool:
        with self.rw_lock_pair.gen_rlock():
            return name in self.table

    def getSlot(self, name: str) -> Slot:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task(subtask_name={}) not deployed".format(name))
            return self.table[name]

