#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-16
import grpc
import logging
import pandas as pd
import time
from typing import List, Dict, Union

from stream_lite.proto import resource_manager_pb2, resource_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase
from .task_manager_client import TaskManagerClient

_LOGGER = logging.getLogger(__name__)


class ResourceManagerClient(ClientBase):

    def __init__(self):
        super(ResourceManagerClient, self).__init__()

    def _init_stub(self, channel):
        return resource_manager_pb2_grpc.ResourceManagerServiceStub(channel)

    def get_client(self, task_manager_name: str) -> TaskManagerClient:
        endpoint = self.getTaskManagerEndpoint(task_manager_name)
        client = TaskManagerClient()
        client.connect(endpoint)
        return client

    def has_task_manager(self, task_manager_name: str) -> bool:
        return self.getTaskManagerEndpoint(task_manager_name) != ""

    def get_host(self, task_manager_name: str) -> str:
        endpoint = self.getTaskManagerEndpoint(task_manager_name)
        return endpoint.split(":")[0]

    def getTaskManagerEndpoint(self, task_manager_name: str) -> str:
        resp = self.stub.getTaskManagerEndpoint(
                resource_manager_pb2.GetTaskManagerEndpointRequest(
                    task_manager_name=task_manager_name))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        return resp.endpoint

    def registerTaskManager(self, endpoint: str, conf: dict) -> str:
        resource_manager_enpoint = conf["resource_manager_enpoint"]
        resp = self.stub.registerTaskManager(
                resource_manager_pb2.RegisterTaskManagerRequest(
                    task_manager_desc=serializator.SerializableTaskManagerDesc.to_proto(
                        host=endpoint.split(":")[0],
                        endpoint=endpoint,
                        name=conf["name"],
                        coord_x=conf["coord"]["x"],
                        coord_y=conf["coord"]["y"],
                        resource=conf["resource"])))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success register task manager(name={})".format(conf["name"]) +\
                " to resource manager(endpoint={})".format(resource_manager_enpoint))
        return resp.job_manager_endpoint

    def heartbeat(self, 
            task_manager_name: str,
            task_manager_endpoint: str,
            coord_x: float,
            coord_y: float,
            peers: Dict[str, int],
            max_nearby_num: int = 3,
            timestamp: Union[None, int] = None) \
                    -> Dict[str, str]:
        if timestamp is None:
            timestamp = util.get_timestamp()

        pb_peers = [resource_manager_pb2.HeartBeatRequest.NearbyPeer(
            name=name, latency=latency) for name, latency in peers.items()]
        resp = self.stub.heartbeat(
                resource_manager_pb2.HeartBeatRequest(
                    endpoint=task_manager_endpoint,
                    name=task_manager_name,
                    coord=common_pb2.Coordinate(
                        x=coord_x, y=coord_y),
                    max_nearby_num=max_nearby_num,
                    timestamp=timestamp,
                    peers=pb_peers))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        nearby_task_managers = {}
        for idx, name in enumerate(resp.names):
            nearby_task_managers[name] = resp.endpoints[idx]
        return nearby_task_managers

    def getAllTaskManagerDesc(self) -> pd.DataFrame:
        resp = self.stub.getAllTaskManagerDesc(common_pb2.NilRequest())

        map_data = {
            "name": [],
            "lon": [],
            "lat": [],
            "endpoint": []
        }
        for desc in resp.task_manager_descs:
            map_data["name"].append(desc.name)
            map_data["lon"].append(desc.coord.x)
            map_data["lat"].append(desc.coord.y)
            map_data["endpoint"].append(desc.endpoint)
        map_df = pd.DataFrame(map_data)

        latency_data = {
            "name": [],
            "peer": [],
            "latency": []
        }
        for desc in resp.task_manager_descs:
            for peer in desc.peers:
                latency_data["name"].append(desc.name)
                latency_data["peer"].append(peer.name)
                latency_data["latency"].append(peer.latency)
        latency_df = pd.DataFrame(latency_data)

        return map_df, latency_df
