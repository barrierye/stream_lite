#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time

from stream_lite.network import serializator
from stream_lite.utils import util

_LOGGER = logging.getLogger(__name__)


class ClientBase(object):

    def __init__(self):
        self.stub = None

    def connect(self, endpoint):
        options = [('grpc.max_receive_message_length', 512 * 1024 * 1024),
                   ('grpc.max_send_message_length', 512 * 1024 * 1024),
                   ('grpc.lb_policy_name', 'round_robin')]
        #  g_endpoint = 'ipv4:{}'.format(','.join(endpoints))
        channel = grpc.insecure_channel(endpoint, options=options)
        self.stub = self._init_stub(channel)

    def _init_stub(self, channel):
        raise NotImplementedError("Failed: function not implemented")
