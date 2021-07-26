#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import inspect
import os
import logging
from typing import List, Dict

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
                    os.path.join(task_dir, task_filename), task_filename),
                locate=task_dict["locate"])
        return task_proto

    @staticmethod
    def from_proto(proto: common_pb2.Task):
        return SerializableTask(
                cls_name=proto.cls_name,
                currency=proto.currency,
                input_tasks=list(proto.input_tasks),
                resources=[SerializableFile.from_proto(f) for f in proto.resources],
                task_file=SerializableFile.from_proto(proto.task_file),
                locate=proto.locate)


class SerializableFile(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableFile, self).__init__(**kwargs)
    
    def instance_to_proto(self) -> common_pb2.File:
        return common_pb2.File(
                name=self.name,
                content=self.content)

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
            host: str,
            endpoint: str,
            name: str,
            coord_x: float,
            coord_y: float,
            resource: dict) -> common_pb2.TaskManagerDescription:
        return common_pb2.TaskManagerDescription(
                host=host,
                endpoint=endpoint,
                name=name,
                coord=SerializableCoordinate.to_proto(
                    x=coord_x, y=coord_y),
                resource=SerializableMachineResource.to_proto(
                    resource))

    @staticmethod
    def from_proto(proto: common_pb2.TaskManagerDescription):
        return SerializableTaskManagerDesc(
                host=proto.host,
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
        return common_pb2.MachineResource(
                slot_number=resource["slot_number"])

    @staticmethod
    def from_proto(proto: common_pb2.MachineResource):
        return SerializableMachineResource(
                slot_number=proto.slot_number)


class SerializableCoordinate(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableCoordinate, self).__init__(**kwargs)

    @staticmethod
    def to_proto(x: float, y: float) -> common_pb2.Coordinate:
        return common_pb2.Coordinate(x=x, y=y)

    @staticmethod
    def from_proto(proto: common_pb2.Coordinate):
        return SerializableCoordinate(
                x=proto.x, y=proto.y)


class SerializableRequiredSlotDesc(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableRequiredSlotDesc, self).__init__(**kwargs)

    @staticmethod
    def to_proto() -> common_pb2.RequiredSlotDescription:
        return common_pb2.RequiredSlotDescription()

    @staticmethod
    def from_proto(proto: common_pb2.RequiredSlotDescription):
        return SerializableRequiredSlotDesc()


class SerializableExectueTask(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableExectueTask, self).__init__(**kwargs)

    def instance_to_proto(self) -> common_pb2.ExecuteTask:
        return SerializableExectueTask.to_proto(
                cls_name=self.cls_name,
                input_endpoints=self.input_endpoints,
                output_endpoints=self.output_endpoints,
                resources=self.resources,
                task_file=self.task_file,
                subtask_name=self.subtask_name,
                partition_idx=self.partition_idx,
                port=self.port)

    @staticmethod
    def to_proto(
            cls_name: str,
            input_endpoints: List[str],
            output_endpoints: List[str],
            resources: List[SerializableFile],
            task_file: SerializableFile,
            subtask_name: str,
            partition_idx: int,
            port: int) -> common_pb2.ExecuteTask:
        return common_pb2.ExecuteTask(
                cls_name=cls_name,
                input_endpoints=input_endpoints,
                output_endpoints=output_endpoints,
                resources=[f.instance_to_proto() for f in resources],
                task_file=task_file.instance_to_proto(),
                subtask_name=subtask_name,
                partition_idx=partition_idx,
                port=port)

    @staticmethod
    def from_proto(proto: common_pb2.ExecuteTask):
        return SerializableExectueTask(
                cls_name=proto.cls_name,
                input_endpoints=list(proto.input_endpoints),
                output_endpoints=list(proto.output_endpoints),
                resources=[SerializableFile.from_proto(res) for res in proto.resources],
                task_file=SerializableFile.from_proto(proto.task_file),
                subtask_name=proto.subtask_name,
                partition_idx=proto.partition_idx,
                port=proto.port)


class SerializableStreamData(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableStreamData, self).__init__(**kwargs)

    def instance_to_proto(self) -> common_pb2.StreamData:
        return common_pb2.StreamData(
                data_id=self.data_id,
                kvs=[kv.instance_to_proto() 
                    for kv in self.kvs],
                timestamp=self.timestamp,
                date_type=self.date_type,
                partition_key=self.partition_key)

    @staticmethod
    def from_proto(proto: common_pb2.StreamData):
        # date_type: common_pb2.StreamData.DataType.XX
        return SerializableStreamData(
                data_id=proto.data_id,
                kvs=[SerializableKeyValueData.from_proto(kv)
                    for kv in proto.kvs],
                timestamp=proto.timestamp,
                date_type=proto.date_type,
                partition_key=proto.partition_key)


class SerializableKeyValueData(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableKeyValueData, self).__init__(**kwargs)

    def instance_to_proto(self) -> common_pb2.StreamData.KeyValueData:
        return common_pb2.StreamData.KeyValueData(
                key=self.key,
                value=self.value)

    @staticmethod
    def from_proto(proto: common_pb2.StreamData.KeyValueData):
        return SerializableKeyValueData(
                key=proto.key,
                value=proto.value)
