#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import os
import time
from typing import List, Dict, Optional, Tuple

from stream_lite import OperatorBase

class SimpleSum(OperatorBase):

    def __init__(self):
        super(SimpleSum, self).__init__()
        self.counter = {}
        self.register_var("counter")

    def init(self, resource_path_dict):
        self.useless_state = str(2**100000) * 500
        self.register_var("useless_state")

    def compute(self, data: str) -> Tuple[str, int]:
        if data not in self.counter:
            self.counter[data] = 0
        self.counter[data] += 1
        #  print((data, self.counter[data]))
        return (data, self.counter[data])
