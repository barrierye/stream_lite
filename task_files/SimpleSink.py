#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
from stream_lite import SinkOperatorBase

class SimpleSink(SinkOperatorBase):

    def init(self):
        print("init sink")

    def compute(self, inputs):
        print(inputs)


