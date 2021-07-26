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
        self._save_resources(
                self.resource_dir, list(execute_task.resources))
        self._save_taskfile(
                self.taskfile_dir, execute_task.task_file)

        self.input_receiver = None
        self.output_dispenser = None
        self._core_process = None

    # --------------------------- init for start service ----------------------------
    def init_for_start_service(self):
        input_channel = self._init_input_receiver(self.input_endpoints)
        output_channel = self._init_output_dispenser(self.output_endpoints)
        self._start_compute_on_standleton_process(
                input_channel, output_channel)

    def _save_resources(self, dirpath: str,
            resources: List[serializator.SerializableFile]) -> None:
        for f in resources:
            f.persistence_to_localfs(dirpath)

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
                    self.cls_name, input_channel, output_channel))
        self._core_process.daemon = True
        self._core_process.start()

    # --------------------------- compute core ----------------------------
    @staticmethod
    def _compute_core( 
            full_task_filename: str,
            cls_name: str,
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue):
        """
        具体执行逻辑
        """
        # import task
        dirpath, filename = os.path.split(full_task_filename)
        module_path = "{}.{}".format(
                dirpath.replace("/", "."), filename.split(".")[0])
        module = importlib.import_module(module_path)
        #TODO

    # --------------------------- pushStreamData (recv) ----------------------------
    def pushStreamData(self, request, context):
        data = serializator.SerializableStreamData.from_proto(request.data)
        pre_subtask = request.from_subtask
        partition_idx = request.partition_idx
        _LOGGER.debug("Recv data(from={}): {}".format(
            pre_subtask, str(request)))
        self.input_receiver.recv_data(partition_idx, data)
        return gen_nil_response()
