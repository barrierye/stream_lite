#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import multiprocessing
from readerwriterlock import rwlock
import logging

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator
from stream_lite.client.task_manager_client import TaskManagerClient

_LOGGER = logging.getLogger(__name__)


class RegisteredTaskManagerTable(object):

    def __init__(self):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.table = {} # name -> RegisteredTaskManager

    def register(self, proto: common_pb2.TaskManagerDescription) -> None:
        task_manager_desc = serializator.SerializableTaskManagerDesc.from_proto(proto)
        with self.rw_lock_pair.gen_wlock():
            name = task_manager_desc.name
            if name in self.table:
                raise KeyError(
                        "Failed to register task manager: name({}) already exists".format(name))
            self.table[name] = RegisteredTaskManager(task_manager_desc)
            _LOGGER.info("Succ register task manager: {}".format(name))

    def hasTaskManager(self, name: str) -> bool:
        with self.rw_lock_pair.gen_rlock():
            return name in self.table

    def get_client(self, name: str) -> TaskManagerClient:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task_manager(name={}) not registered".format(name))
            return self.table[name].get_client()

    def get_host(self, name: str) -> str:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task_manager(name={}) not registered".format(name))
            return self.table[name].get_host()


class RegisteredTaskManager(object):

    def __init__(self, task_manager_desc):
        self.task_manager_desc = task_manager_desc
        self.client = self._init_client()

    def _init_client(self) -> TaskManagerClient:
        client = TaskManagerClient()
        client.connect(self.task_manager_desc.endpoint)
        _LOGGER.debug(
                "Succ init register task manager(name={}) client"
                .format(self.task_manager_desc.name))
        return client

    def get_client(self) -> TaskManagerClient:
        return self.client

    def get_host(self) -> str:
        return self.task_manager_desc.host
