#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25
import os
from concurrent import futures
import grpc
import threading
import logging
import multiprocessing

from stream_lite.utils import AvailablePortGenerator

_LOGGER = logging.getLogger(__name__)


class ServerBase(object):

    def __init__(self, rpc_port, worker_num):
        self.rpc_port = rpc_port
        self.worker_num = worker_num
        if self.rpc_port != -1 and \
                not AvailablePortGenerator.port_is_available(self.rpc_port):
            raise ValueError(
                    "Failed: port({}) is not available".format(self.rpc_port))
        self._process = None
        self.service_name = None

    def init_service(self, server):
        raise NotImplemented("Failed: function not implemented")

    def update_rpc_port(self, port):
        self.rpc_port = port
        _LOGGER.debug(
                "[{}] Attention: rpc_port has been updated({})".format(
                    self.service_name, self.rpc_port))

    def _inner_run(self):
        server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self.worker_num),
                options=[('grpc.max_send_message_length', 512 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 512 * 1024 * 1024)])
        self.init_service(server)

        # 一些 Server 在初始化时候需要更新端口
        if self.rpc_port == -1:
            raise ValueError(
                    "Failed: port({}) must be 0-65535".format(self.rpc_port))
        server.add_insecure_port('[::]:{}'.format(self.rpc_port))
        _LOGGER.info("[{}] Run on port: {}".format(self.service_name, self.rpc_port))
        server.start()
        server.wait_for_termination()

    def run(self):
        try:
            self._inner_run()
        except Exception as e:
            _LOGGER.critical(
                    "Failed to run service ({})".format(e), exc_info=True)
            os._exit(-1)

    def run_on_standalone_process(self, is_process):
        if self._process is not None:
            raise RuntimeError(
                    "Failed: process already running")
        if is_process:
            # 这里不能将 daemon 设为 True:
            #    AssertionError: daemonic processes are not allowed to have children
            self._process = multiprocessing.Process(
                    target=self.run,
                    daemon=False)
        else:
            self._process = threading.Thread(
                    target=self.run)
        self._process.start()
