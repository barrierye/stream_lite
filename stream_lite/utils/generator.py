#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
from contextlib import closing
import socket
import os
import uuid


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class GeneratorBase(metaclass=SingletonMeta):

    def __init__(self):
        pass

    def next(self):
        raise NotImplementedError("Failed: function not implemented")


class AvailablePortGenerator(GeneratorBase):

    def __init__(self, start_port=12000):
        super(AvailablePortGenerator, self).__init__()
        self.curr_port = start_port

    @staticmethod
    def port_is_available(port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(2)
            result = sock.connect_ex(('0.0.0.0', port))
        return result != 0

    def next(self):
        while not AvailablePortGenerator.port_is_available(self.curr_port):
            self.curr_port += 1
        available_port = self.curr_port
        self.curr_port += 1
        return available_port


class JobIdGenerator(GeneratorBase):

    def __init__(self):
        super(JobIdGenerator, self).__init__()
        
    def next(self):
        uid = uuid.uuid1()
        return uid.hex


class IncIdGenerator(GeneratorBase):

    def __init__(self):
        super(IncIdGenerator, self).__init__()
        self.counter = 0

    def next(self):
        count = self.counter
        self.counter += 1
        return count


class CheckpointIdGenerator(IncIdGenerator):

    def __init__(self):
        super(CheckpointIdGenerator, self).__init__()


class DataIdGenerator(IncIdGenerator):

    def __init__(self):
        super(DataIdGenerator, self).__init__()
