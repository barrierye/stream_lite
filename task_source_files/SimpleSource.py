#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import time
import re

from stream_lite import SourceOperatorBase

class SimpleSource(SourceOperatorBase):

    def init(self, resource_path_dict):
        def generator():
            with open("document.txt") as f:
                for line in f:
                    words = re.split('[.,\s]', line)
                    for word in words:
                        if word:
                            yield word.lower()
        self.iter = iter(generator())

    def compute(self, inputs):
        #  time.sleep(1)
        return next(self.iter)
