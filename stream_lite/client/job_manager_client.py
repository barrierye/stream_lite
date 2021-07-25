#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class JobManagerClient(ClientBase):

    def __init__(self):
        super(JobManagerClient, self).__init__()

    def _init_stub(self, channel):
        return job_manager_pb2_grpc.JobManagerServiceStub(channel)

    def registerTaskManager(self, conf: dict):
        job_manager_enpoint = conf["job_manager_enpoint"]
        resp = self.stub.registerTaskManager(
                job_manager_pb2.RegisterTaskManagerRequest(
                    task_manager_desc=common_pb2.TaskManagerDescription(
                        endpoint=job_manager_enpoint,
                        name=conf["name"],
                        coord=common_pb2.Coordinate(
                            x=conf["coord"]["x"],
                            y=conf["coord"]["y"]),
                        resource=common_pb2.MachineResource())))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success register task manager(name={})".format(conf["name"]) +\
                " to job manager(endpoint={})".format(job_manager_enpoint))
