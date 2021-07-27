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

def get_filename(url: str) -> str:
    """
    example:
        url: /path/to/file
        path: /path/to/
        filename: file
    """
    path, filename = os.path.split(url)
    return filename

def get_timestamp() -> int:
    return int(time.time())
