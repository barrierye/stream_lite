#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import inspect
import pickle
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

    def check_attrs(self, required_attrs: list):
        for attr in required_attrs:
            if not hasattr(self, attr):
                raise AttributeError(
                        "Failed: {} must have attribute {}".format(
                            type(self), attr))

class SerializableTask(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableTask, self).__init__(**kwargs)
        required_attrs = ["cls_name", "currency", "input_tasks",
            "resources", "task_file", "locate"]
        self.check_attrs(required_attrs)

    @staticmethod
    def to_proto(task_dict: dict, task_dir: str) -> common_pb2.Task:
        task_filename = "{}.py".format(task_dict["name"])
        task_proto = common_pb2.Task(
                cls_name=task_dict["name"],
                currency=task_dict.get("currency", 1),
                input_tasks=task_dict.get("input_tasks", []),
                resources=[
                    SerializableFile.to_proto(
                        r, util.get_filename(r)) 
                    for r in task_dict.get("resources", [])],
                locate=task_dict["locate"])
        from stream_lite.task_manager.task.operator import BUILDIN_OPS
        if task_dict["name"] not in BUILDIN_OPS:
            task_proto.task_file.CopyFrom(
                    SerializableFile.to_proto(
                        os.path.join(
                            task_dir, task_filename), 
                        task_filename))
        return task_proto

    @staticmethod
    def from_proto(proto: common_pb2.Task):
        task_file = None
        from stream_lite.task_manager.task.operator import BUILDIN_OPS
        if proto.cls_name not in BUILDIN_OPS:
            task_file = SerializableFile.from_proto(proto.task_file)
        return SerializableTask(
                cls_name=proto.cls_name,
                currency=proto.currency,
                input_tasks=list(proto.input_tasks),
                resources=[SerializableFile.from_proto(f) for f in proto.resources],
                task_file=task_file,
                locate=proto.locate)


class SerializableFile(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableFile, self).__init__(**kwargs)
        required_attrs = ["name", "content"]
        self.check_attrs(required_attrs)
    
    def instance_to_proto(self) -> common_pb2.File:
        return common_pb2.File(
                name=self.name,
                content=self.content)

    @staticmethod
    def to_proto(path: str, name: str) -> common_pb2.File:
        with open(path, "rb") as f:
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
        with open(filename, "wb") as f:
            f.write(self.content)
        _LOGGER.debug(
                "Success persistence to localfs: {}".format(filename))
        return True


class SerializableTaskManagerDesc(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableTaskManagerDesc, self).__init__(**kwargs)
        required_attrs = ["host", "endpoint", "name", "coord", "resource", "home_path"]
        self.check_attrs(required_attrs)

    def instance_to_proto(self) -> common_pb2.TaskManagerDescription:
        return SerializableTaskManagerDesc.to_proto(
                host=self.host,
                endpoint=self.endpoint,
                name=self.name,
                coord_x=self.coord.x,
                coord_y=self.coord.y,
                home_path=self.home_path,
                resource=self.resource.instance_to_dict())

    @staticmethod
    def to_proto(
            host: str,
            endpoint: str,
            name: str,
            coord_x: float,
            coord_y: float,
            home_path: str,
            resource: dict) -> common_pb2.TaskManagerDescription:
        return common_pb2.TaskManagerDescription(
                host=host,
                endpoint=endpoint,
                name=name,
                home_path=home_path,
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
                home_path=proto.home_path,
                coord=SerializableCoordinate.from_proto(
                    proto.coord),
                resource=SerializableMachineResource.from_proto(
                    proto.resource))


class SerializableMachineResource(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableMachineResource, self).__init__(**kwargs)
        required_attrs = ["slot_number"]
        self.check_attrs(required_attrs)

    def instance_to_dict(self) -> dict:
        resource = {"slot_number": self.slot_number}
        return resource

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
        required_attrs = ["x", "y"]
        self.check_attrs(required_attrs)

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
        required_attrs = []
        self.check_attrs(required_attrs)

    @staticmethod
    def to_proto() -> common_pb2.RequiredSlotDescription:
        return common_pb2.RequiredSlotDescription()

    @staticmethod
    def from_proto(proto: common_pb2.RequiredSlotDescription):
        return SerializableRequiredSlotDesc()


class SerializableExectueTask(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableExectueTask, self).__init__(**kwargs)
        required_attrs = ["cls_name", "input_endpoints", 
                "output_endpoints", "resources", "task_file",
                "subtask_name", "partition_idx", "port",
                "upstream_cls_names", "downstream_cls_names",
                "streaming_name"]
        self.check_attrs(required_attrs)

    def instance_to_proto(self) -> common_pb2.ExecuteTask:
        return SerializableExectueTask.to_proto(
                cls_name=self.cls_name,
                input_endpoints=self.input_endpoints,
                output_endpoints=self.output_endpoints,
                resources=self.resources,
                task_file=self.task_file,
                subtask_name=self.subtask_name,
                partition_idx=self.partition_idx,
                port=self.port,
                upstream_cls_names=self.upstream_cls_names,
                downstream_cls_names=self.downstream_cls_names,
                streaming_name=self.streaming_name)

    @staticmethod
    def to_proto(
            cls_name: str,
            input_endpoints: List[str],
            output_endpoints: List[str],
            resources: List[SerializableFile],
            task_file: SerializableFile,
            subtask_name: str,
            partition_idx: int,
            port: int,
            upstream_cls_names: List[str],
            downstream_cls_names: List[str],
            streaming_name: str) -> common_pb2.ExecuteTask:
        proto = common_pb2.ExecuteTask(
                cls_name=cls_name,
                input_endpoints=input_endpoints,
                output_endpoints=output_endpoints,
                resources=[f.instance_to_proto() for f in resources],
                subtask_name=subtask_name,
                partition_idx=partition_idx,
                port=port,
                upstream_cls_names=upstream_cls_names,
                downstream_cls_names=downstream_cls_names,
                streaming_name=streaming_name)
        if task_file is not None:
            proto.task_file.CopyFrom(task_file.instance_to_proto())
        return proto

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
                port=proto.port,
                upstream_cls_names=list(proto.upstream_cls_names),
                downstream_cls_names=list(proto.downstream_cls_names),
                streaming_name=proto.streaming)


class SerializableRecord(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableRecord, self).__init__(**kwargs)
        required_attrs = ["data_id", "data", "timestamp", 
                "data_type", "partition_key"]
        self.check_attrs(required_attrs)

    def instance_to_proto(self) -> common_pb2.Record:
        return common_pb2.Record(
                data_id=self.data_id,
                data=self.data.instance_to_bytes(),
                data_type=self.data_type,
                timestamp=self.timestamp,
                partition_key=self.partition_key)

    @staticmethod
    def from_proto(proto: common_pb2.Record):
        return SerializableRecord(
                data_id=proto.data_id,
                data=SerializableData.from_bytes(proto.data, proto.data_type),
                data_type=proto.data_type,
                timestamp=proto.timestamp,
                partition_key=proto.partition_key)


class SerializableData(SerializableObject):

    def __init__(self, **kwargs):
        super(SerializableData, self).__init__(**kwargs)
        required_attrs = ["data_type", "data"]
        self.check_attrs(required_attrs)

    def instance_to_bytes(self):
        byte_array = None
        if self.data_type == common_pb2.Record.DataType.PICKLE:
            byte_array = pickle.dumps(self.data)
        else:
            build_in_events = [
                    common_pb2.Record.DataType.CHECKPOINT,
                    common_pb2.Record.DataType.FINISH,
                    common_pb2.Record.DataType.MIGRATE,
                    common_pb2.Record.DataType.TERMINATE_SUBTASK,
                    common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE]
            if self.data_type in build_in_events:
                byte_array = self.data.SerializeToString()
            else:
                raise TypeError("Failed: unknow data type({})".format(data_type))
        return byte_array

    @staticmethod
    def from_bytes(byte_array: bytes, data_type: common_pb2.Record.DataType):
        data = None
        if data_type == common_pb2.Record.DataType.PICKLE:
            data = pickle.loads(byte_array)
        else:
            build_in_events = {
                    common_pb2.Record.DataType.CHECKPOINT: common_pb2.Record.Checkpoint,
                    common_pb2.Record.DataType.FINISH: common_pb2.Record.Finish,
                    common_pb2.Record.DataType.MIGRATE: common_pb2.Record.Migrate,
                    common_pb2.Record.DataType.TERMINATE_SUBTASK: common_pb2.Record.TerminateSubtask,
                    common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE: \
                            common_pb2.Record.CheckpointPrepareForMigrate}
            if data_type in build_in_events:
                cls = build_in_events[data_type]
                data = cls()
                data.ParseFromString(byte_array)
            else:
                raise TypeError("Failed: unknow data type({})".format(data_type))
        return SerializableData(
                data_type=data_type,
                data=data)
    
    @staticmethod
    def from_object(data, data_type=common_pb2.Record.DataType.PICKLE):
        return SerializableData(
                data_type=data_type,
                data=data)
