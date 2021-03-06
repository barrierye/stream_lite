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
import stream_lite.proto.resource_manager_pb2 as resource_manager_pb2
from stream_lite.network import serializator
from stream_lite.client.task_manager_client import TaskManagerClient
from stream_lite.resource_manager.peer_latency_table import PeerLatencyTable

_LOGGER = logging.getLogger(__name__)


class RegisteredTaskManagerTable(object):

    def __init__(self, latency_table: PeerLatencyTable):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.table = {} # name -> RegisteredTaskManager
        self.latency_table = latency_table

    def register(self, proto: common_pb2.TaskManagerDescription) -> None:
        task_manager_desc = serializator.SerializableTaskManagerDesc.from_proto(proto)
        with self.rw_lock_pair.gen_wlock():
            name = task_manager_desc.name
            if name in self.table:
                raise KeyError(
                        "Failed to register task manager: name({}) already exists".format(name))
            self.table[name] = RegisteredTaskManager(
                    name, task_manager_desc, self.latency_table)
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
        with self.rw_lock_pair.gen_wlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task_manager(name={}) not registered".format(name))
            self.table[name].update_coordinate(coord)

    def update_task_manager_nearby_peers(self,
            name: str,
            peers: List[resource_manager_pb2.HeartBeatRequest.NearbyPeer]) -> None:
        """
        ?????? task_manager ?????? peer ???????????????
        """
        with self.rw_lock_pair.gen_wlock():
            if name not in self.table:
                raise KeyError(
                        "Failed: task_manager(name={}) not registered".format(name))
            self.table[name].update_nearby_peers(peers)

    def get_nearby_task_manager(self, 
            task_manager_name: str,
            coord: common_pb2.Coordinate,
            max_nearby_num: int) -> \
                    Tuple[List[str], List[str]]:
        """
        ????????????????????????????????? max_nearby_num ??? task_manager
        """
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

    def get_all_task_manager_descs(self) -> List[common_pb2.TaskManagerDescription]:
        resp = []
        with self.rw_lock_pair.gen_rlock():
            for name, registered_task_manager in self.table.items():
                desc = registered_task_manager.get_description_with_latency()
                resp.append(desc)
        return resp


class RegisteredTaskManager(object):

    def __init__(self, name: str, task_manager_desc, latency_table: PeerLatencyTable):
        self.name = name
        self.task_manager_desc = task_manager_desc
        self.task_manager_endpoint = self.task_manager_desc.endpoint
        self.coord = self.task_manager_desc.coord
        latency_table.add_task_manager(name)
        self.latency_table = latency_table

    def get_description_with_latency(self) -> common_pb2.TaskManagerDescription:
        desc = self.task_manager_desc.instance_to_proto()
        # add latency
        latency_dict = self.latency_table.get_latency(self.name)
        for peer_name, latency in latency_dict.items():
            peer = desc.peers.add()
            peer.name = peer_name
            peer.latency = latency
        return desc

    def get_endpoint(self) -> str:
        return self.task_manager_endpoint

    def update_coordinate(self, coord: common_pb2.Coordinate) -> None:
        self.coord.x = coord.x
        self.coord.y = coord.y

    def get_distance(self, coord: common_pb2.Coordinate) -> float:
        horizontal_dist = (self.coord.x - coord.x) ** 2
        vertical_dist = (self.coord.y - coord.y) ** 2
        return math.sqrt(horizontal_dist + vertical_dist)

    def update_nearby_peers(self,
            peers: List[resource_manager_pb2.HeartBeatRequest.NearbyPeer]) -> None:
        self.latency_table.update_latency(self.name, peers)
