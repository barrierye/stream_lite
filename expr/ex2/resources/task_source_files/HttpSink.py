#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import os
import requests
from time import time, sleep

from stream_lite import SinkOperatorBase

class HttpSink(SinkOperatorBase):

    def compute(self, inputs):
        #self.fout.write("{}\n".format(inputs))
        #self.fout.flush()
        while True:
            a = requests.get(
                    "http://192.168.105.83:8998/api/recv")
            if a.status_code == 200:
                break
            sleep(0.01)
