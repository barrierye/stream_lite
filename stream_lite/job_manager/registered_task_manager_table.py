#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import multiprocessing
import logging

import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)

class RegisteredTaskManagerTable(object):

    def __init__(self):
        self.lock = multiprocessing.Lock()
        self.table = {} # name -> task_manager_desc

    def register(self, proto: common_pb2.TaskManagerDescription) -> None:
        task_manager_desc = serializator.SerializableTaskManagerDesc.from_proto(proto)
        with self.lock:
            name = task_manager_desc.name
            if name in self.table:
                raise KeyError(
                        "Failed to register: name({}) already exists".format(name))
            self.table[name] = task_manager_desc
            _LOGGER.info("Succ register task manager: {}".format(name))

