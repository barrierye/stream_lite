#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import multiprocessing
from time import time as current_timestamp
import time
import threading
import logging

from stream_lite.client import heartbeat_client

_LOGGER = logging.getLogger(__name__)


class HeartbeatUtil(object):

    def __init__(self, timeout):
        self.lock = threading.Lock()
        self.timeout = timeout
        self.table = {} # addr: timestamp
        self.client = heartbeat_manager_client.HeartbeatManagerClient()
        self._thread = None

    def register(self, tm_addr):
        with self.lock:
            if tm_addr in self.table:
                raise ValueError(
                        "Failed to register: tm_addr({}) already exists".format(tm_addr))
            self.table[tm_addr] = current_timestamp()

    def resetHeartbeat(self, tm_addr):
        with self.lock:
            if tm_addr not in self.table:
                raise ValueError(
                        "Failed to set heartbeat: tm_addr({}) not register".format(tm_addr))
            self.table[tm_addr] = current_timestamp()

    def start(self, callback):
        self._thread = threading.Thread(
                target=self._inner_check_func, 
                args=(self.timeout, callback))
        self._thread.start()
        _LOGGER.info("Succ to start heartbeat thread")

    def _inner_check_func(self, interval, callback):
        while True:
            ctime = current_timestamp()
            with self.lock:
                for k, v in self.table.items():
                    if v + self.timeout < ctime:
                        _LOGGER.error("Failed to connect task_manager: timeout for {}".format(k))
                        # TODO: timeout
            time.sleep(interval)
