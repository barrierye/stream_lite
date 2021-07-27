#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import time

from stream_lite import SourceOperatorBase

class SimpleSource(SourceOperatorBase):

    def init(self):
        print("init source")

    def compute(self, inputs):
        time.sleep(1)
        return "source"
