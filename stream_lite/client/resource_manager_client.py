#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-16
import grpc
import logging
import time

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
