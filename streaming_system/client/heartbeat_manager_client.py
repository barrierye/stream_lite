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


class HeartbeatManagerClient(ClientBase):

    def __init__(self):
        super(HeartbeatManagerClient, self).__init__()
        pass

    def notifyHeartbeatTimeout(self, task_id):
        # TODO
        pass
