#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import inspect
import os
import importlib

from stream_lite.proto import job_manager_pb2

class Serializator(object):

    def __init__(self):
        pass

    @staticmethod
    def to_proto(cls):
        filename = inspect.getsourcefile(cls)
        with open(filename) as f:
            file_str = f.read()
        task_proto = job_manager_pb2.TaskClass(
                filename=filename,
                file_str=file_str,
                cls_name=cls.__name__)
        return task_proto

    @staticmethod
    def from_proto(proto):
        tmp_dir = "tmp"
        os.system("mkdir -p {} && touch {}/__init__.py"
                .format(tmp_dir, tmp_dir))
        filename = proto.filename
        file_str = proto.file_str
        cls_name = proto.cls_name
        with open(os.path.join(tmp_dir, filename), "w") as f:
            f.write(file_str)
        modellib = importlib.import_module(
                "{}.{}".format(tmp_dir, filename.split('.')[0]))
        cls = getattr(modellib, cls_name)
        return cls
