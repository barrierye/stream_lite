#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
from stream_lite import Task

class SimpleSink(Task):
    def init(self, name):
        print("init sink")

    def run(self, inputs):
        return inputs


