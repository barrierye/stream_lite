#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import os
import time

from stream_lite import OperatorBase

class SimpleSum(OperatorBase):

    def __init__(self):
        super(SumOp, self).__init__()
        self.counter = {}
        self.register_var("counter")

    def compute(self, data: str) -> Tuple[str, int]:
        if data not in self.counter:
            self.counter[data] = 0
        self.counter[data] += 1
        time.sleep(0.01)
        #  print((data, self.counter[data]))
        return (data, self.counter[data])
