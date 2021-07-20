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


class Client(object):

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
        raise NotImplementedError("")

    def resetHeartbeat(self, task_id):
        raise NotImplementedError("")

    def notifyHeartbeatTimeout(self, task_id):
        raise NotImplementedError("")


class UserClient(Client):

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


class HeartbeatManagerClient(Client):

    def __init__(self):
        super(HeartbeatManagerClient, self).__init__()
        pass

    def notifyHeartbeatTimeout(self, task_id):
        # TODO
        pass
