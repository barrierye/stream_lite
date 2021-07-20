#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time

from proto import job_manager_pb2, job_manager_pb2_grpc
from network import serializator
from utils import util
from .client import ClientBase

_LOGGER = logging.getLogger(__name__)


class TaskManagerClient(Client):

    def __init__(self):
        super(UserClient, self).__init__()

    def resetHeartbeat(self, task_id):
        req = job_manager_pb2.HeartbeatRequest(
                addr="{}:{}".format(self.ip, self.self_port),
                timestamp=time.time(),
                task_id=task_id)
        resp = self.stub.resetHeartbeat(req)
        print(str(resp))
