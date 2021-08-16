#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-18
import multiprocessing
import logging
import time

from stream_lite.proto import common_pb2
from stream_lite.client import ResourceManagerClient

_LOGGER = logging.getLogger(__name__)


class HeartBeatHelper(object):
    """
    工具类（仅被 task_manager 使用）
    1. 监控 gps 信息
    2. 探测 task_manager 间通信延迟
    2. 汇报给 resource_manager
    """

    def __init__(self,
            task_manager_name: str,
            task_manager_endpoint: str,
            coord: common_pb2.Coordinate,
            resource_manager_enpoint: str,
            ):
        self.name = task_manager_name
        self.endpoint = task_manager_endpoint
        self.coord = coord
        self.resource_manager_enpoint = resource_manager_enpoint
        self._process = None

    def _inner_run(self, 
            name: str,
            endpoint: str,
            coord: common_pb2.Coordinate,
            resource_manager_enpoint: str) -> None: 
        resource_manager_client = ResourceManagerClient()
        resource_manager_client.connect(resource_manager_enpoint)
        peers = {} # name -> client
        while True:
            # TODO: 监控 gps 信息
            new_coord = coord
            
            # 探测 task_manager 间通信延迟
            peer_latencies = {} # name -> latency
            for name, client in peers.items():
                latency = client.testLatency()
                peer_latencies[name] = latency

            # 汇报给 resource_manager
            resource_manager_client.heartbeat(
                    task_manager_name=name,
                    task_manager_endpoint=endpoint,
                    coord_x=coord.x,
                    coord_y=coord.y,
                    peers=peer_latencies)
            # sleep for 5 sec
            time.sleep(5)


    def run(self, 
            name: str,
            endpoint: str,
            coord: common_pb2.Coordinate,
            resource_manager_enpoint: str) -> None:
        try:
            self._inner_run(
                    name, endpoint, coord, resource_manager_enpoint)
        except Exception as e:
            _LOGGER.critical(
                    "Failed to run heartbeat helper ({})".format(e), exc_info=True)
            os._exit(-1)

    def run_on_standalone_process(self, is_process: bool) -> None:
        if self._process is not None:
            raise RuntimeError(
                    "Failed: process already running")
        if is_process:
            # 这里不能将 daemon 设为 True:
            #    AssertionError: daemonic processes are not allowed to have children
            self._process = multiprocessing.Process(
                    target=self.run, 
                    args=(
                        self.name, 
                        self.endpoint, 
                        self.coord,
                        self.resource_manager_enpoint),
                    daemon=False)
        else:
            self._process = threading.Thread(
                    target=self.run,
                    args=(
                        self.name,
                        self.endpoint,
                        self.coord,
                        self.resource_manager_enpoint))
        self._process.start()
