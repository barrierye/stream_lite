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
from stream_lite.job_manager import scheduler

from .peer_latency_table import PeerLatencyTable
from .execute_task_table import ExecuteTaskTable
from .registered_task_manager_table import RegisteredTaskManagerTable
from .strategy.greedy_strategy import GreedyStrategy

_LOGGER = logging.getLogger(__name__)


class ResourceManagerServicer(resource_manager_pb2_grpc.ResourceManagerServiceServicer):

    def __init__(self, job_manager_endpoint: str):
        super(ResourceManagerServicer, self).__init__()
        self.latency_table = PeerLatencyTable() # 与每个节点的延迟记录
        self.execute_task_table = ExecuteTaskTable() # 记录每个task在哪个节点 TODO: 仅单个jobid
        self.registered_task_manager_table = RegisteredTaskManagerTable(self.latency_table)
        self.job_manager_endpoint = job_manager_endpoint
        self.last_migrate_info_list = [] # 迁移后通知RM修改信息

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

    # --------------------------- registerJobExecuteInfo ----------------------------
    def registerJobExecuteInfo(self, request, context):
        try:
            self.execute_task_table.build_from_rpc_request(request)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=str(e))
        return gen_nil_response()

    # --------------------------- getAutoMigrateSubtasks ----------------------------
    def getAutoMigrateSubtasks(self, request, context):
        jobid = request.jobid
        
        # 自动迁移逻辑
        _LOGGER.info("analyzing execute path...")
        try:
            migrate_info_list, diff = GreedyStrategy.get_migrate_infos(
                    jobid,
                    self.execute_task_table.get_infos(),
                    self.latency_table)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            migrate_info_list = []
            diff = -1

        print(migrate_info_list)
        self.last_migrate_info_list = migrate_info_list
        '''
        # mock
        return resource_manager_pb2.GetAutoMigrateSubtasksResponse(
                status=common_pb2.Status(),
                infos=[common_pb2.MigrateInfo(
                    src_cls_name="SumOp",
                    target_task_manager_locate="TM_2",
                    jobid=jobid,
                    src_currency=2,
                    src_partition_idx=0)])
        '''
        return resource_manager_pb2.GetAutoMigrateSubtasksResponse(
                status=common_pb2.Status(),
                infos=migrate_info_list,
                latency_diff=diff)

    # --------------------------- doMigrateLastTime ----------------------------
    def doMigrateLastTime(self, request, context):
        migrate_info_list = self.last_migrate_info_list

        # update execute_task_table
        for migrate_info in migrate_info_list:
            subtask_name = scheduler.Scheduler._get_subtask_name(
                    migrate_info.src_cls_name, 0, migrate_info.src_currency) # TODO: index=0
            info = self.execute_task_table.get_info(subtask_name)
            info.task_manager_name = migrate_info.target_task_manager_locate
            self.execute_task_table.update_exec_task_info(subtask_name, info)
        
        self.last_migrate_info_list = []
        return gen_nil_response()
