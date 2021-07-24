#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import logging

import stream_lite.proto.job_manager_pb2_grpc as job_manager_pb2_grpc
from stream_lite.server.server_base import ServerBase
from stream_lite.job_manager.job_manager import JobManagerServicer

_LOGGER = logging.getLogger(__name__)


class JobManager(ServerBase):

    def __init__(self, rpc_port, worker_num=1):
        super(JobManager, self).__init__(rpc_port, worker_num)

    def init_service(self, server):
        job_manager_pb2_grpc.add_JobManagerServiceServicer_to_server(
                 JobManagerServicer(), server)
