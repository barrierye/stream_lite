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

    def get_latency(self, src_name: Union[str, None] = None,
            dst_name: Union[str, None] = None) -> dict:
        """
        有三种用法：
        1. src_name 和 dst_name 均为 None, 返回整张表
        2. src_name 不为 None，返回 src_name 的子表
        3. src_name 和 dst_name 均不为 None，返回两者之间的 latency
        """
        with self.rw_lock_pair.gen_rlock():
            if src_name is not None:
                if src_name not in self.latencies:
                    raise KeyError(
                            "Failed to get latency: src {} does not exist".format(src_name))
                if dst_name is not None:
                    if src_name == dst_name:
                        return 0.0
                    if dst_name not in self.latencies[src_name]:
                        raise KeyError(
                                "Failed to get latency: dst {} does not exist".format(dst_name))
                    return self.latencies[src_name][dst_name]
                else:
                    return self.latencies[src_name]
            else:
                return self.latencies
