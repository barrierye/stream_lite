#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import grpc
import logging
import pickle
import inspect

import stream_lite.proto.job_manager_pb2 as job_manager_pb2
import stream_lite.proto.common_pb2 as common_pb2
import stream_lite.proto.job_manager_pb2_grpc as job_manager_pb2_grpc
from stream_lite.network.util import gen_nil_response
from stream_lite.network import serializator
from . import scheduler

from .registered_task_manager_table import RegisteredTaskManagerTable

_LOGGER = logging.getLogger(__name__)


class JobManagerServicer(job_manager_pb2_grpc.JobManagerServiceServicer):

    def __init__(self):
        super(JobManagerServicer, self).__init__()
        self.registered_task_manager_table = RegisteredTaskManagerTable()
        self.scheduler = scheduler.UserDefinedScheduler(
                self.registered_task_manager_table)

    def submitJob(self, request, context):
        persistence_dir = "./_tmp/server/task_files"
        seri_tasks = []
        for task in request.tasks:
            seri_task = serializator.SerializableTask.from_proto(task)
            seri_task.task_file.persistence_to_localfs(persistence_dir)
            seri_tasks.append(seri_task)
        
        try:
            resp = self._innerSubmitJob(seri_tasks)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(err_code=1, message=e)
        return resp 
        
    def _innerSubmitJob(self, seri_tasks):
        # schedule
        execute_map = self.scheduler.schedule(seri_tasks)
        
        # deploy
        for task_manager_name, seri_execute_tasks in execute_map.items():
            client = self.registered_task_manager_table.get_client(task_manager_name)
            for execute_task in seri_execute_tasks:
                resp = client.deployTask(
                        task_manager_pb2.DeployTaskRequest(
                            exec_task=execute_task.to_proto()))
                if resp.status.err_code != 0:
                    raise Exception(resp.status.message)

        # TODO: start: 从末尾往前 start, 确保 output 的 rpc 服务已经起来

        return gen_nil_response()

    def registerTaskManager(self, request, context):
        try:
            self.registered_task_manager_table.register(
                    request.task_manager_desc)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=e)
        return gen_nil_response()

