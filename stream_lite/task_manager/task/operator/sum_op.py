#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
from typing import Tuple

from .operator_base import OperatorBase

class SumOp(OperatorBase):

    def __init__(self):
        super(SumOp, self).__init__()
        self.counter = {}
        self.register_var("counter")

    def compute(self, data: str) -> Tuple[str, int]:
        if data not in self.counter:
            self.counter[data] = 0
        self.counter[data] += 1
        return (data, self.counter[data])
