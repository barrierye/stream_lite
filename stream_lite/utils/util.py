#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from contextlib import closing
import socket
import os
import time
import requests

def get_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

"""
def get_ip() -> str:
    # 获取公网ip
    ip = requests.get("http://ifconfig.me/ip", timeout=1).text.strip()
    return ip
"""

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
    # ms
    return int(round(time.time() * 1000))


class FinishJobError(RuntimeError):
    """
    用于表示正常终止 Job 的异常
    """

    def __init__(self, info_str):
        super(FinishJobError, self).__init__(info_str)
