#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from grpc_tools import protoc
import os

def gen_proto_code(filename):
    protoc.main((
        '',
        '-I.',
        '--python_out=.',
        '--grpc_python_out=.',
        filename, ))

def gen_proto_codes():
    dirpath = "stream_lite/proto"
    files = ["job_manager.proto", 
             "common.proto",
             "task_manager.proto",
             "subtask.proto"]
    for filename in files:
        gen_proto_code(
                os.path.join(dirpath, filename))


if __name__ == '__main__':
    gen_proto_codes()
