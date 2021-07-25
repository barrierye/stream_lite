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
        resp = self._innerSubmitJob(seri_tasks)
        return resp 
        
    def _innerSubmitJob(self, seri_tasks):
        # schedule
        try:
            schedule_map = self.scheduler.schedule(seri_tasks)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(err_code=1, message=e)
        
        # require slot
        for task_manager_name, seri_tasks in schedule_map.items():
            client = self.registered_task_manager_table.get_client(task_manager_name)
            resp = client.requestSlot(
                    task_manager_pb2.RequiredSlotRequest(
                        slot_descs=[
                            serializator.SerializableRequiredSlotDesc.to_proto()
                            for task in seri_tasks]))
            if resp.status.err_code != 0:
                _LOGGER.error(resp.status.message, exc_info=True)
                return gen_nil_response(err_code=1, message=resp.status.message)

        # deploy

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

