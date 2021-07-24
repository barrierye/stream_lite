#-*- coding:utf8 -*-
# Copyright (c) 2020 barriery
# Python release: 3.7.0
# Create time: 2020-04-04
from setuptools import setup, find_packages
from grpc_tools import protoc
from stream_lite.proto import gen_code

# run proto codegen
gen_code.gen_proto_codes()

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
