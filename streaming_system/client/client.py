#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect

from proto import job_manager_pb2, job_manager_pb2_grpc
from network import serializator

_LOGGER = logging.getLogger(__name__)


class Client(object):

    def __init__(self):
        self.stub = None

    def connect(self, endpoints):
        options = [('grpc.max_receive_message_length', 512 * 1024 * 1024),
                   ('grpc.max_send_message_length', 512 * 1024 * 1024),
                   ('grpc.lb_policy_name', 'round_robin')]
        g_endpoint = 'ipv4:{}'.format(','.join(endpoints))
        channel = grpc.insecure_channel(g_endpoint, options=options)
        self.stub = job_manager_pb2_grpc.JobManagerServiceStub(channel)

    def submitJob(self, cls):
        filename = inspect.getsourcefile(cls)
        with open(filename) as f:
            file_str = f.read()
        req = job_manager_pb2.Request(
                logid=100,
                tasks=[serializator.Serializator.to_proto(cls)])
        resp = self.stub.submitJob(req)
        print(str(resp))
