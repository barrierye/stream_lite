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
        self.registered_vars = set() # 用于 snapshot 的状态

    def set_name(self, name):
        self.name = name

    def register_var(self, var_name):
        self.registered_vars.add(var_name)

    def register_vars(self, var_names):
        for var_name in var_names:
            self.register_var(var_name)

    def init(self, resource_path_dict: Dict[str, str]):
        pass

    def compute(self, data):
        raise NotImplementedError("Failed: function not implemented")

    def checkpoint(self):
        """
        返回值为 snapshot 的状态，将被作为文件存储 common_pb2.File
        """
        state = {var_name: getattr(self, var_name) for var_name in self.registered_vars}
        return state
