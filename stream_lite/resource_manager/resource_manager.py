#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import os
import grpc
import logging
import pickle
import inspect
from typing import List, Dict, Tuple, Set

import stream_lite.proto.resource_manager_pb2 as resource_manager_pb2
import stream_lite.proto.common_pb2 as common_pb2
import stream_lite.proto.resource_manager_pb2_grpc as resource_manager_pb2_grpc

from stream_lite import client
from stream_lite.network.util import gen_nil_response
from stream_lite.network import serializator
from stream_lite.utils import JobIdGenerator, EventIdGenerator

from .registered_task_manager_table import RegisteredTaskManagerTable

_LOGGER = logging.getLogger(__name__)


class ResourceManagerServicer(resource_manager_pb2_grpc.ResourceManagerServiceServicer):

    def __init__(self, job_manager_endpoint: str):
        super(ResourceManagerServicer, self).__init__()
        self.registered_task_manager_table = RegisteredTaskManagerTable()
        self.job_manager_endpoint = job_manager_endpoint

    # --------------------------- register task manager ----------------------------
    def registerTaskManager(self, request, context):
        try:
            self.registered_task_manager_table.register(
                    request.task_manager_desc)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return resource_manager_pb2.RegisterTaskManagerResponse(
                    status=common_pb2.Status(
                        err_code=1, message=str(e)),
                    job_manager_endpoint="")
        return resource_manager_pb2.RegisterTaskManagerResponse(
                job_manager_endpoint=self.job_manager_endpoint,
                status=common_pb2.Status())
    
    # --------------------------- get task manager endpoint ----------------------------
    def getTaskManagerEndpoint(self, request, context):
        try:
            task_manager_name = request.task_manager_name
            endpoint = self.registered_task_manager_table.get_task_manager_endpoint(task_manager_name)
        except KeyError:
            return resource_manager_pb2.GetTaskManagerEndpointResponse(
                    endpoint="", state=common_pb2.Status())
        except Exception as e:
            return resource_manager_pb2.GetTaskManagerEndpointResponse(
                    state=common_pb2.Status(
                        err_code=1, message=str(e)))
        return resource_manager_pb2.GetTaskManagerEndpointResponse(
                endpoint=endpoint, status=common_pb2.Status())
