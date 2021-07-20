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
from stream_lite.network import serializator
from stream_lite.utils import util
from stream_lite.client.client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class HeartbeatClient(ClientBase):

    def __init__(self):
        super(HeartbeatClient, self).__init__()
        pass

    def notifyHeartbeatTimeout(self, task_id):
        # TODO
        pass
