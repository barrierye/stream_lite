#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import multiprocessing
from readerwriterlock import rwlock
import logging
import math
from typing import List, Dict, Union, Optional, Tuple

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

    def has_task_manager(self, name: str) -> bool:
        with self.rw_lock_pair.gen_rlock():
            return name in self.table

    def get_task_manager_endpoint(self, name: str) -> str:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                 raise KeyError(
                         "Failed: task_manager(name={}) not registered".format(name))
            return self.table[name].get_endpoint()

    def get_task_manager_ip(self, name: str) -> str:
        endpoint = self.get_task_manager_endpoint(name)
        return endpoint.split(":")[0]

    def update_task_manager_coordinate(self,
            name: str, 
            coord: common_pb2.Coordinate) -> None:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task_manager(name={}) not registered".format(name))
            self.table[name].update_coordinate(coord)

    def get_nearby_task_manager(self, 
            task_manager_name: str,
            coord: common_pb2.Coordinate,
            max_nearby_num: int) -> \
                    Tuple[List[str], List[str]]:
        with self.rw_lock_pair.gen_rlock():
            dist_map = {} # name: dist
            for name, registered_task_manager in self.table.items():
                if name == task_manager_name:
                    continue
                dist_map[name] = registered_task_manager.get_distance(coord)
            nearly_task_managers = sorted(dist_map.items(), key=lambda x: x[1])
            nearly_task_manager_names = [x[0] \
                    for x in nearly_task_managers[:max_nearby_num]]
            nearly_task_manager_endpoints = [self.table[name].get_endpoint() \
                    for name in nearly_task_manager_names]
            return nearly_task_manager_names, nearly_task_manager_endpoints


class RegisteredTaskManager(object):

    def __init__(self, task_manager_desc):
        self.task_manager_desc = task_manager_desc
        self.task_manager_endpoint = self.task_manager_desc.endpoint

    def get_endpoint(self) -> str:
        return self.task_manager_endpoint

    def update_coordinate(self, coord: common_pb2.Coordinate) -> None:
        self.task_manager_desc.coord.x = coord.x
        self.task_manager_desc.coord.y = coord.y

    def get_distance(self, coord: common_pb2.Coordinate) -> float:
        self_coord = self.task_manager_desc.coord
        horizontal_dist = (self_coord.x - coord.x) ** 2
        vertical_dist = (self_coord.y - coord.y) ** 2
        return math.sqrt(horizontal_dist + vertical_dist)
