#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import xxhash

from .key_op_base import KeyOperatorBase

class KeyByInputOp(KeyOperatorBase):

    def __init__(self):
        super(KeyByInputOp, self).__init__()

    def compute(self, data: str) -> int:
        """ return key """
        hashed_key = xxhash.xxh32_intdigest(data, seed=19260817)
        return hashed_key
