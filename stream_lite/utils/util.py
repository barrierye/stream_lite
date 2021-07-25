#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from contextlib import closing
import socket
import os

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

def port_is_available(port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(2)
        result = sock.connect_ex(('0.0.0.0', port))
    return result != 0
