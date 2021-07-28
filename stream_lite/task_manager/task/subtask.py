#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
"""
Slot 中实际执行的 SubTask
"""
import os
import logging
import importlib
import multiprocessing
from typing import List, Dict

from stream_lite.proto import subtask_pb2_grpc

from stream_lite.network import serializator
from stream_lite.network.util import gen_nil_response
from stream_lite.client import SubTaskClient
from stream_lite.utils import util

from .input_receiver import InputReceiver
from .output_dispenser import OutputDispenser
import operator

_LOGGER = logging.getLogger(__name__)


class SubTaskServicer(subtask_pb2_grpc.SubTaskServiceServicer):
    """
    rpc   == data ==>  InputReceiver (rpc service process & standalone process) 
          == data ==>  channel (multiprocessing.Queue)
          == data ==>  compute core (standalone process)
          == data ==>  channel (multiprocessing.Queue)
          == data ==>  OutputDispenser (standalone process)
          == data ==>  rpc
    """

    def __init__(self, 
            tm_name: str,
            execute_task: serializator.SerializableExectueTask):
        super(SubTaskServicer, self).__init__()
        self.tm_name = tm_name
        self.subtask_id = id(self)
        self.cls_name = execute_task.cls_name
        self.task_filename = execute_task.task_file.name
        self.input_endpoints = execute_task.input_endpoints
        self.output_endpoints = execute_task.output_endpoints
        self.subtask_name = execute_task.subtask_name
        self.partition_idx = execute_task.partition_idx
        self.port = execute_task.port
        self.resource_dir = "_tmp/tm/{}/{}_{}/resource".format(
                self.tm_name, self.cls_name, self.partition_idx)
        self.taskfile_dir = "_tmp/tm/{}/{}_{}/taskfile".format(
                self.tm_name, self.cls_name, self.partition_idx)
        self.resource_path_dict = self._save_resources(
                self.resource_dir, list(execute_task.resources))
        if self.cls_name not in operator.BUILDIN_OPS:
            self._save_taskfile(
                    self.taskfile_dir, execute_task.task_file)

        self.input_receiver = None
        self.output_dispenser = None
        self._core_process = None

    def _save_resources(self, dirpath: str,
            resources: List[serializator.SerializableFile]) -> None:
        resource_path_dict = {}
        for f in resources:
            f.persistence_to_localfs(dirpath)
            resource_path_dict[f.name] = os.path.join(dirpath, f.name)
        return resource_path_dict

    def _save_taskfile(self, dirpath: str,
            taskfile: serializator.SerializableFile) -> None:
        taskfile.persistence_to_localfs(dirpath)
        # 创建 __init__.py
        while True:
            dirpath, _ = os.path.split(dirpath)
            if not dirpath:
                break
            os.system("touch {}".format(
                os.path.join(dirpath, "__init__.py")))

    # --------------------------- init for start service ----------------------------
    def init_for_start_service(self):
        input_channel = None
        output_channel = None
        if not self._is_source_op():
            input_channel = self._init_input_receiver(
                    self.input_endpoints)
        if not self._is_sink_op():
            output_channel = self._init_output_dispenser(
                    self.output_endpoints)
        self._start_compute_on_standleton_process(
                input_channel, output_channel)

    def _is_source_op(self) -> bool:
        cls = SubTaskServicer._import_cls_from_file(
                self.cls_name, os.path.join(
                    self.taskfile_dir, self.task_filename))
        return issubclass(cls, operator.SourceOperatorBase)

    def _is_sink_op(self) -> bool:
        cls = SubTaskServicer._import_cls_from_file(
                self.cls_name, os.path.join(
                    self.taskfile_dir, self.task_filename))
        return issubclass(cls, operator.SinkOperatorBase)

    def _init_input_receiver(self, input_endpoints: List[str]) \
            -> multiprocessing.Queue:
        input_channel = multiprocessing.Queue()
        self.input_receiver = InputReceiver(
                input_channel, input_endpoints)
        return input_channel

    def _init_output_dispenser(self, output_endpoints: List[str]) \
            -> multiprocessing.Queue:
        output_channel = multiprocessing.Queue()
        self.output_dispenser = OutputDispenser(
                output_channel, output_endpoints,
                self.subtask_name, self.partition_idx)
        return output_channel

    def _start_compute_on_standleton_process(self,
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue):
        if self._core_process is not None:
            raise SystemExit("Failed: process already running")
        self._core_process = multiprocessing.Process(
                target=SubTaskServicer._compute_core, 
                args=(
                    os.path.join(
                        self.taskfile_dir, self.task_filename), 
                    self.cls_name, self.subtask_name, 
                    self.resource_path_dict, 
                    input_channel, output_channel))
        self._core_process.daemon = True
        self._core_process.start()

    # --------------------------- compute core ----------------------------
    @staticmethod
    def _compute_core( 
            full_task_filename: str,
            cls_name: str,
            subtask_name: str,
            resource_path_dict: Dict[str, str],
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue):
        try:
            SubTaskServicer._inner_compute_core(
                    full_task_filename, cls_name, subtask_name,
                    resource_path_dict, input_channel, output_channel)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            raise SystemExit("Failed: run {} task failed".format(subtask_name))

    @staticmethod
    def _inner_compute_core( 
            full_task_filename: str,
            cls_name: str,
            subtask_name: str,
            resource_path_dict: Dict[str, str],
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue):
        """
        具体执行逻辑
        """
        cls = SubTaskServicer._import_cls_from_file(
                cls_name, full_task_filename)
        if not issubclass(cls, operator.OperatorBase):
            raise SystemExit(
                    "Failed: {} is not a subclass of OperatorBase".format(cls_name))
        is_source_op = issubclass(cls, operator.SourceOperatorBase)
        is_sink_op = issubclass(cls, operator.SinkOperatorBase)
        is_key_op = issubclass(cls, operator.KeyOperatorBase)
        task_instance = cls()
        task_instance.set_name(subtask_name)
        task_instance.init(resource_path_dict)

        while True:
            data_id = None
            timestamp = None
            partition_key = ""

            input_data = None
            if not is_source_op:
                record = input_channel.get() # SerializableRecord
                input_data = record.data.data # SerializableData.data
                data_id = record.data_id
                timestamp = record.timestamp
                _LOGGER.debug("[In] {}: {}".format(cls_name, input_data))
            else:
                data_id = "data_id" # TODO: 进程安全 gen
                timestamp = util.get_timestamp()
            
            output_data = task_instance.compute(input_data)
            _LOGGER.debug("[Out] {}: {}".format(cls_name, output_data))
            if is_key_op:
                partition_key = output_data
                output_data = input_data
            
            if not is_sink_op:
                output = serializator.SerializableData.from_object(output_data)
                output_channel.put(
                        serializator.SerializableRecord(
                            data_id=data_id,
                            data_type=output.data_type,
                            data=output,
                            timestamp=timestamp,
                            partition_key=partition_key))

    @staticmethod
    def _import_cls_from_file(cls_name: str, filepath: str):
        if cls_name in operator.BUILDIN_OPS:
            return operator.BUILDIN_OPS[cls_name]
        if not filepath.endswith(".py"):
            raise SystemExit(
                    "Failed: can not import module '{}'".format(filepath))
        dirpath, filename = os.path.split(filepath)
        module_path = "{}.{}".format(
                dirpath.replace("/", "."), filename.split(".")[0])
        module = importlib.import_module(module_path)
        cls = getattr(module, cls_name)
        return cls

    # --------------------------- pushRecord (recv) ----------------------------
    def pushRecord(self, request, context):
        pre_subtask = request.from_subtask
        partition_idx = request.partition_idx
        record = request.record
        _LOGGER.debug("Recv data(from={}): {}".format(
            pre_subtask, str(request)))
        self.input_receiver.recv_data(partition_idx, record)
        return gen_nil_response()
