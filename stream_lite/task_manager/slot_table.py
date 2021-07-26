#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import multiprocessing
from readerwriterlock import rwlock
import logging

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class SlotTable(object):

    def __init__(self, capacity: int):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.capacity = capacity
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
            self.table[subtask_name] = Slot(execute_task)
            _LOGGER.debug("Succ deploy task: {}".format(subtask_name))

    def hasSlot(self, name: str) -> bool:
        with self.rw_lock_pair.gen_rlock():
            return name in self.table

    def getSlot(self, name: str) -> Slot:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task(subtask_name={}) not deployed".format(name))
            return self.table[name]


class Slot(object):

    def __init__(self, execute_task: serializator.SerializableExectueTask):
        self.execute_task = execute_task
        self.status = "DEPLOYED"

    def __str__(self):
        return "[{}] subtask_name: {}, status: {}".format(
                self.cls_name, 
                self.execute_task.subtask_name, 
                self.status)
