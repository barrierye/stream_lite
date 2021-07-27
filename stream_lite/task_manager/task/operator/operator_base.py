#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义算子的基类
"""

class OperatorBase(object):

    def __init__(self):
        pass

    def init(self):
        pass

    def compute(self, data):
        raise NotImplementedError("Failed: function not implemented")
