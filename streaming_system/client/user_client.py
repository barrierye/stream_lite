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
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class UserClient(ClientBase):

    def __init__(self):
        super(UserClient, self).__init__()

    def submitJob(self, cls):
        filename = inspect.getsourcefile(cls)
        with open(filename) as f:
            file_str = f.read()
        req = job_manager_pb2.SubmitJobRequest(
                logid=100,
                tasks=[serializator.Serializator.to_proto(cls)])
        resp = self.stub.submitJob(req)
        print(str(resp))
