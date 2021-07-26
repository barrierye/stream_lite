#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging

from stream_lite.proto import subtask_pb2_gprc
from stream_lite.server.server_base import ServerBase
from stream_lite.task_manager.task.sub_task import SubTaskServicer
from stream_lite.utils import AvailablePortGenerator

_LOGGER = logging.getLogger(__name__)


class SubTaskServer(ServerBase):

    def __init__(self, 
            tm_name: str,
            execute_task: serializator.SerializableExectueTask,
            rpc_port: int = -1, 
            worker_num: int = 1):
        if rpc_port != -1:
            raise ValueError(
                    "Failed: can not set rpc_port for SubTaskServer")
        super(SubTaskServer, self).__init__(rpc_port, worker_num)
        self.tm_name = tm_name
        self.execute_task = execute_task

    def init_service(self, server):
        subtask_service = SubTaskServicer(
                self.tm_name, self.execute_task)
        # **Attention**: motify rpc port
        self.update_rpc_port(subtask_service.port)
        subtask_service.init_for_start_service()
        subtask_pb2_gprc.add_SubTaskServiceServicer_to_server(
                subtask_service, server)