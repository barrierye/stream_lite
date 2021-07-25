#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging

import stream_lite.proto.task_manager_pb2_grpc as task_manager_pb2_grpc
from stream_lite.server.server_base import ServerBase
from stream_lite.task_manager.task_manager import TaskManagerServicer

_LOGGER = logging.getLogger(__name__)


class TaskManager(ServerBase):

    def __init__(self, conf_yaml_path: str, rpc_port: int, worker_num=1):
        super(TaskManager, self).__init__(rpc_port, worker_num)
        self.conf_yaml_path = conf_yaml_path

    def init_service(self, server):
        task_manager_pb2_grpc.add_TaskManagerServiceServicer_to_server(
                 TaskManagerServicer(self.rpc_port, self.conf_yaml_path), server)
