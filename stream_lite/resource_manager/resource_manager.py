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
import pandas as pd
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
                    endpoint="", status=common_pb2.Status())
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return resource_manager_pb2.GetTaskManagerEndpointResponse(
                    state=common_pb2.Status(
                        err_code=1, message=str(e)))
        return resource_manager_pb2.GetTaskManagerEndpointResponse(
                endpoint=endpoint, status=common_pb2.Status())

    # --------------------------- heartbeat(query nearby task manager) ----------------------------
    def heartbeat(self, request, context):
        try:
            timestamp = request.timestamp
            task_manager_name = request.name
            coord = request.coord
            max_nearby_num = request.max_nearby_num
            peers = request.peers
            self.registered_task_manager_table.update_task_manager_coordinate(
                    task_manager_name, coord)
            self.registered_task_manager_table.update_task_manager_nearby_peers(
                    task_manager_name, peers)
            neary_task_manager_names, neary_task_manager_endpoints \
                    = self.registered_task_manager_table.get_nearby_task_manager(
                            task_manager_name, coord, max_nearby_num)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return resource_manager_pb2.HeartBeatResponse(
                    status=common_pb2.Status(
                        err_code=1, message=str(e)),
                    nearby_size=0)
        nearby_size = len(neary_task_manager_names)
        return resource_manager_pb2.HeartBeatResponse(
                status=common_pb2.Status(),
                nearby_size=nearby_size,
                names=neary_task_manager_names,
                endpoints=neary_task_manager_endpoints)

    # --------------------------- getAllTaskManagerDesc (by streamlit)  ----------------------------
    def getAllTaskManagerDesc(self, request, context):
        all_descs = self.registered_task_manager_table.get_all_task_manager_descs()
        return resource_manager_pb2.GetAllTaskManagerDescResponse(
                status=common_pb2.Status(),
                task_manager_descs=all_descs)

    # --------------------------- getAutoMigrateSubtasks ----------------------------
    def getAutoMigrateSubtasks(self, request, context):
        jobid = request.jobid
        
        # TODO
        # 自动迁移逻辑

        # mock
        return resource_manager_pb2.GetAutoMigrateSubtasksResponse(
                status=common_pb2.Status(),
                infos=[common_pb2.MigrateInfo(
                    src_cls_name="SumOp",
                    target_task_manager_locate="TM_2",
                    jobid=jobid,
                    src_currency=2,
                    src_partition_idx=0)])
