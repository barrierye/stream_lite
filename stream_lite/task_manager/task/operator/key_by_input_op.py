#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义 Source 算子的基类
"""

from .key_op_base import KeyOperatorBase


class KeyByInputOp(KeyOperatorBase):

    def __init__(self):
        pass

    def compute(self, data: str) -> str:
        """ return key """
        return data
