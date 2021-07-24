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

_LOGGER = logging.getLogger(__name__)


class ClientBase(object):

    def __init__(self, self_port=None):
        self.stub = None
        self.ip = util.get_ip()
        self.self_port = self_port

    def connect(self, endpoints):
        options = [('grpc.max_receive_message_length', 512 * 1024 * 1024),
                   ('grpc.max_send_message_length', 512 * 1024 * 1024),
                   ('grpc.lb_policy_name', 'round_robin')]
        g_endpoint = 'ipv4:{}'.format(','.join(endpoints))
        channel = grpc.insecure_channel(g_endpoint, options=options)
        self.stub = job_manager_pb2_grpc.JobManagerServiceStub(channel)

    def submitJob(self, cls):
        raise NotImplementedError("Failed: function not implemented")
