#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time
from typing import List, Dict

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
from stream_lite.proto import task_manager_pb2, task_manager_pb2_grpc
from stream_lite.proto import subtask_pb2, subtask_pb2_grpc
from stream_lite.proto import common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class SubTaskClient(ClientBase):

    def __init__(self):
        super(SubTaskClient, self).__init__()

    def _init_stub(self, channel):
        return subtask_pb2_grpc.SubTaskServiceStub(channel)

    def pushStreamData(self, proto: common_pb2.StreamData):
        resp = self.stub.pushStreamData(proto)
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)
