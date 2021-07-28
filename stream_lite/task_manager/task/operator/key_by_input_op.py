#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义 Source 算子的基类
"""

from .key_op_base import KeyOperatorBase
import xxhash

class KeyByInputOp(KeyOperatorBase):

    def __init__(self):
        pass

    def compute(self, data: str) -> int:
        """ return key """
        hashed_key = xxhash.xxh32_intdigest(data, seed=19260817)
        print("key: {}".format(hashed_key))
        return hashed_key
