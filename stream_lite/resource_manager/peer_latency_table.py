#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
from readerwriterlock import rwlock
from typing import List, Dict, Union
from stream_lite.proto import resource_manager_pb2


class PeerLatencyTable(object):

    def __init__(self):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.latencies = {} # task_manager: {name: latency}

    def add_task_manager(self, name: str) -> None:
        with self.rw_lock_pair.gen_wlock():
            if name in self.latencies:
                raise KeyError(
                        "Failed to add task manager: {} already exists".format(name))
            self.latencies[name] = {}
        
    def update_latency(self, 
            name: str,
            peers: List[resource_manager_pb2.HeartBeatRequest.NearbyPeer]) -> None:
        with self.rw_lock_pair.gen_wlock():
            for peer in peers:
                self.latencies[name][peer.name] = peer.latency

    def get_latency(self, name: Union[str, None] = None) -> dict:
        with self.rw_lock_pair.gen_rlock():
            if name:
                if name not in self.latencies:
                    raise KeyError(
                            "Failed to get latency: {} does not exist".format(name))
                return self.latencies[name]
            else:
                return self.latencies
