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
import stream_lite.proto.job_manager_pb2_grpc as job_manager_pb2_grpc
from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class JobManagerServicer(job_manager_pb2_grpc.JobManagerServiceServicer):

    def __init__(self):
        super(JobManagerServicer, self).__init__()

    def submitJob(self, request, context):
        _LOGGER.debug("get req: {}".format(request.logid))
        for task in request.tasks:
            seri_task = serializator.SerializableTask.from_proto(task)
            seri_task.task_file.persistence_to_localfs("./_tmp/server/task_files")
        resp = job_manager_pb2.SubmitJobResponse()
        return resp
