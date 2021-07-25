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
from stream_lite.proto import task_manager_pb2, task_manager_pb2_grpc
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class TaskManagerClient(ClientBase):

    def __init__(self):
        super(TaskManagerClient, self).__init__()

    def _init_stub(self, channel):
        return task_manager_pb2_grpc.TaskManagerServiceStub(channel)