#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
from concurrent import futures
import grpc
import logging
import multiprocessing

from stream_lite.utils import AvailablePortGenerator

_LOGGER = logging.getLogger(__name__)


class ServerBase(object):

    def __init__(self, rpc_port, worker_num):
        self.rpc_port = rpc_port
        self.worker_num = worker_num
        if not AvailablePortGenerator.port_is_available(rpc_port):
            raise ValueError(
                    "Failed: port({}) is not available".format(rpc_port))
        self._process = None

    def init_service(self, server):
        raise NotImplemented("Failed: function not implemented")

    def update_rpc_port(self, port):
        self.rpc_port = port

    def run(self):
        server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self.worker_num),
                options=[('grpc.max_send_message_length', 256 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 256 * 1024 * 1024)])
        self.init_service(server)
        server.add_insecure_port('[::]:{}'.format(self.rpc_port))
        _LOGGER.info("Run on port: {}".format(self.rpc_port))
        server.start()
        server.wait_for_termination()

    def run_on_standalone_process(self):
        if self._process is not None:
            raise RuntimeError(
                    "Failed: process already running")
        self._process = multiprocessing.Process(
                target=self.run, args=(, ))
        # stop self when main process stop
        self._process.darmon = True
        self._process.start()
