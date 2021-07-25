#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import inspect
import os
import importlib
import logging

from stream_lite.proto import common_pb2
from stream_lite.utils import util


_LOGGER = logging.getLogger(__name__)


class SerializableObject(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    @staticmethod
    def to_proto(*args, **kwargs):
        raise NotImplementedError("Failed: function not implemented")

    @staticmethod
    def from_proto(proto: object):
        raise NotImplementedError("Failed: function not implemented")


class SerializableTask(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableTask, self).__init__(**kwargs)

    @staticmethod
    def to_proto(task_dict: dict, task_dir: str) -> common_pb2.Task:
        task_filename = "{}.py".format(task_dict["name"])
        task_proto = common_pb2.Task(
                cls_name=task_dict["name"],
                currency=task_dict["currency"],
                input_tasks=task_dict["input_tasks"],
                resources=[
                    SerializableFile.to_proto(
                        r, util.get_filename(r)) for r in task_dict["resources"]],
                task_file=SerializableFile.to_proto(
                    os.path.join(task_dir, task_filename), task_filename))
        return task_proto

    @staticmethod
    def from_proto(proto: common_pb2.Task):
        return SerializableTask(
                cls_name=proto.cls_name,
                currency=proto.currency,
                input_tasks=list(proto.input_tasks),
                resources=list(proto.resources),
                task_file=SerializableFile.from_proto(proto.task_file))


class SerializableFile(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableFile, self).__init__(**kwargs)
    
    @staticmethod
    def to_proto(path: str, name: str) -> common_pb2.File:
        with open(path) as f:
            file_str = f.read()
        file_proto = common_pb2.File(
                name=name,
                content=file_str)
        return file_proto

    @staticmethod
    def from_proto(proto: common_pb2.File):
        return SerializableFile(
                name=proto.name, 
                content=proto.content)

    def persistence_to_localfs(self, prefix_path: str) -> bool:
        os.system("mkdir -p {}".format(prefix_path))
        filename = os.path.join(prefix_path, self.name)
        with open(filename, "w") as f:
            f.write(self.content)
        _LOGGER.debug(
                "Success persistence to localfs: {}".format(filename))
        return True


class SerializableTaskManagerDesc(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableTaskManagerDesc, self).__init__(**kwargs)

    @staticmethod
    def to_proto(
            endpoint: str,
            name: str,
            coord_x: float,
            coord_y: float,
            resource: dict) -> common_pb2.TaskManagerDescription:
        return common_pb2.TaskManagerDescription(
                endpoint=endpoint,
                name=name,
                coord=SerializableCoordinate.to_proto(
                    x=coord_x, y=coord_y),
                resource=SerializableMachineResource.to_proto(
                    resource))

    @staticmethod
    def from_proto(proto: common_pb2.TaskManagerDescription):
        return SerializableTaskManagerDesc(
                endpoint=proto.endpoint,
                name=proto.name,
                coord=SerializableCoordinate.from_proto(
                    proto.coord),
                resource=SerializableMachineResource.from_proto(
                    proto.resource))


class SerializableMachineResource(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableMachineResource, self).__init__(**kwargs)

    @staticmethod
    def to_proto(resource: dict) -> common_pb2.MachineResource:
        return common_pb2.MachineResource()

    @staticmethod
    def from_proto(proto: common_pb2.MachineResource):
        return SerializableMachineResource()


class SerializableCoordinate(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableCoordinate, self).__init__(**kwargs)

    @staticmethod
    def to_proto(x: float, y: float) -> common_pb2.Coordinate:
        return common_pb2.Coordinate(x=x, y=y)

    @staticmethod
    def from_proto(proto: common_pb2.Coordinate):
        return SerializableCoordinate(
                x=proto.x,
                y=proto.y)
