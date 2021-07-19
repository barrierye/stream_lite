#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import multiprocessing

from client import 

class HeartbeatManager(object):

    def __init__(self, timeout):
        self.lock = multiprocessing.Lock()
        self.timeout = timeout
        self.table = {} # addr: timestamp
        self.client = HeartbeatManagerClient

    def register(self, client_addr):
        with self.lock:
            if client_addr in self.table:
                raise ValueError(
                        "Failed to register: client_addr({}) already exists".format(client_addr))
            self.table[client_addr] = current_timestamp()


