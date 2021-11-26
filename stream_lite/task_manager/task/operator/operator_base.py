#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
"""
用户自定义算子的基类
"""
from typing import Dict, List, Tuple, Any

class OperatorBase(object):

    def __init__(self):
        self.registered_vars = set() # 用于 snapshot 的状态

    def set_name(self, name: str) -> None:
        self.name = name

    def register_var(self, var_name: str) -> None:
        self.registered_vars.add(var_name)

    def register_vars(self, var_names: List[str]) -> None:
        for var_name in var_names:
            self.register_var(var_name)

    def init(self, resource_path_dict: Dict[str, str]):
        pass

    def compute(self, data: Any) -> Any:
        raise NotImplementedError("Failed: function not implemented")

    def checkpoint(self) -> Dict[str, Any]:
        """
        返回值为 snapshot 的状态，将被作为文件存储 common_pb2.File
        """
        state = {var_name: getattr(self, var_name) for var_name in self.registered_vars}
        return state

    def restore_from_checkpoint(self, state: Dict[str, Any]) -> None:
        for key, value in state.items():
            if key not in self.registered_vars:
                raise KeyError("Failed: variable not registered")
            setattr(self, key, value)

    def close(self):
        return
