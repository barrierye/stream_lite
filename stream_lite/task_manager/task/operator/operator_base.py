#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义算子的基类
"""
from typing import Dict

class OperatorBase(object):

    def __init__(self):
        pass

    def set_name(self, name):
        self.name = name

    def init(self, resource_path_dict: Dict[str, str]):
        pass

    def compute(self, data):
        raise NotImplementedError("Failed: function not implemented")
