#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-15
import logging

import stream_lite.proto.resource_manager_pb2_grpc as resource_manager_pb2_grpc
from stream_lite.server.server_base import ServerBase
from stream_lite.resource_manager.resource_manager import ResourceManagerServicer

_LOGGER = logging.getLogger(__name__)


class ResourceManager(ServerBase):

    def __init__(self, 
            job_manager_enpoint: str,
            rpc_port: int, 
            worker_num: int = 4):
        super(ResourceManager, self).__init__(rpc_port, worker_num)
        self.job_manager_enpoint = job_manager_enpoint
        self.service_name = "Service@ResourceManager"

    def init_service(self, server):
        resource_manager_pb2_grpc.add_ResourceManagerServiceServicer_to_server(
                 ResourceManagerServicer(self.job_manager_enpoint), server)
