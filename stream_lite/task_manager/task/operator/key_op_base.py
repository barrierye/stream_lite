#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义 Source 算子的基类
"""

from .operator_base import OperatorBase


class KeyOperatorBase(OperatorBase):

    def __init__(self):
        super(KeyOperatorBase, self).__init__()

    def compute(self, data) -> str:
        """ return key """
        raise NotImplementedError("Failed: function not implemented")
