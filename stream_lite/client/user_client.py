#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time
import yaml
import importlib

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
from stream_lite.network import serializator
from stream_lite.utils import util
from stream_lite.client.client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class UserClient(ClientBase):

    def __init__(self):
        super(UserClient, self).__init__()

    def submitJob(self, yaml_path):
        with open(yaml_path) as f:
            conf = yaml.load(f.read(), Loader=yaml.FullLoader)

        req = job_manager_pb2.SubmitJobRequest(
                logid=100,
                tasks=[
                    serializator.SerializableTask.to_proto(
                        task_dict, conf["task_files"]) 
                    for task_dict in conf["tasks"]])
        resp = self.stub.submitJob(req)
        print(str(resp))
