#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from contextlib import closing
import socket
import os
import time

def get_ip() -> str:
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip

def get_filename(uri: str) -> str:
    """
    example:
        uri: /path/to/file
        path: /path/to/
        filename: file
    """
    path, filename = os.path.split(uri)
    return filename

def get_dirname(uri: str) -> str:
    path, filename = os.path.split(uri)
    return path

def get_timestamp() -> int:
    return int(time.time())


class FinishJobError(RuntimeError):
    """
    用于表示正常终止 Job 的异常
    """

    def __init__(self, info_str):
        super(FinishJobError, self).__init__(info_str)
