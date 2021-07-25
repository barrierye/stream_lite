#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import grpc
import logging
import yaml

import stream_lite.proto.task_manager_pb2_grpc as task_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.client import JobManagerClient
import stream_lite.utils.util

_LOGGER = logging.getLogger(__name__)


class TaskManagerServicer(task_manager_pb2_grpc.TaskManagerServiceServicer):

    def __init__(self, rpc_port: int, conf_yaml_path: str):
        super(TaskManagerServicer, self).__init__()
        self.job_manager_client = None
        self.endpoint = "{}:{}".format(
                stream_lite.utils.util.get_ip(), rpc_port)
        with open(conf_yaml_path) as f:
            self.conf = yaml.load(f.read(), Loader=yaml.Loader)
        self._register()

    def _register(self):
        job_manager_enpoint = self.conf["job_manager_enpoint"]
        self.job_manager_client = JobManagerClient()
        _LOGGER.debug("Try connect to job manager: {}".format(job_manager_enpoint))
        self.job_manager_client.connect(job_manager_enpoint)
        _LOGGER.debug("Try register task manager")
        self.job_manager_client.registerTaskManager(
                self.endpoint, self.conf)

    def requestSlot(self, request, context):
        pass

    def deployTask(self, request, context):
        pass

    
