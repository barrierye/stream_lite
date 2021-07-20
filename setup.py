#-*- coding:utf8 -*-
# Copyright (c) 2020 barriery
# Python release: 3.7.0
# Create time: 2020-04-04
from setuptools import setup, find_packages
from grpc_tools import protoc

# run proto codegen
proto_files = ["stream_lite/proto/job_manager.proto"]
for f in proto_files:
    protoc.main((
        '',
        '-I.',
        '--python_out=.',
        '--grpc_python_out=.',
        f, ))

setup(
    name='stream_lite',
    packages=find_packages(where='.'),
    version='0.0.0',
    description='',
    keywords='',
    author='barriery',
    url='',
    install_requires=[
        'protobuf>=3.12.2',
    ],
)
