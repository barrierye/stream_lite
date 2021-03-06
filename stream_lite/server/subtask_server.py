#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging
from typing import List, Dict, Union

from stream_lite.proto import subtask_pb2_grpc
from stream_lite.proto import common_pb2
from stream_lite.proto import task_manager_pb2

from stream_lite.network import serializator
from stream_lite.server.server_base import ServerBase
from stream_lite.task_manager.task.subtask import SubTaskServicer
from stream_lite.utils import AvailablePortGenerator

_LOGGER = logging.getLogger(__name__)


class SubTaskServer(ServerBase):

    def __init__(self, 
            tm_name: str,
            jobid: str,
            job_manager_enpoint: str,
            execute_task: serializator.SerializableExectueTask,
            rpc_port: int = -1,
            worker_num: int = 4,
            state: Union[None, task_manager_pb2.DeployTaskRequest.State] = None):
        if rpc_port != -1:
            raise ValueError(
                    "Failed: can not set rpc_port for SubTaskServer")
        super(SubTaskServer, self).__init__(rpc_port, worker_num)
        self.tm_name = tm_name
        self.jobid = jobid
        self.job_manager_enpoint = job_manager_enpoint
        self.execute_task = execute_task
        self.service_name = self.execute_task.subtask_name
        self.state = state

    def init_service(self, server):
        subtask_service = SubTaskServicer(
                self.tm_name, self.jobid, 
                self.job_manager_enpoint, 
                self.execute_task,
                self.state)
        # **Attention**: motify rpc port
        self.update_rpc_port(subtask_service.port)
        try:
            subtask_service.init_for_start_service()
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            raise SystemExit(
                    "Failed: init for service({}) error".format(
                        self.execute_task.subtask_name))
        subtask_pb2_grpc.add_SubTaskServiceServicer_to_server(
                subtask_service, server)
