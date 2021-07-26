#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
"""
Slot 中实际执行的 SubTask
"""
import logging
import multiprocessing
from typing import List, Dict

from stream_lite.proto import subtask_pb2_grpc

from stream_lite.network import serializator
from stream_lite.client import SubTaskClient

from .input_receiver import InputReceiver
from .output_dispenser import OutputDispenser

_LOGGER = logging.getLogger(__name__)


class SubTaskServicer(subtask_pb2_grpc.SubTaskServiceServicer):
    """
    rpc   == data ==>  InputReceiver  
          == data ==>  channel 
          == data ==>  Core  
          == data ==>  channel 
          == data ==>  OutputDispenser
          == data ==>  rpc
    """

    def __init__(self, 
            tm_name: str,
            execute_task: serializator.SerializableExectueTask):
        super(SubTaskServicer, self).__init__()
        self.tm_name = tm_name
        self.subtask_id = id(self)
        self.cls_name = execute_task.cls_name
        self.input_endpoints = execute_task.input_endpoints
        self.output_endpoints = execute_task.output_endpoints
        self.subtask_name = execute_task.subtask_name
        self.partition_idx = execute_task.partition_idx
        self.port = execute_task.port
        self.input_channel = None
        self.input_receiver = None
        self.output_channel = None
        self.output_dispenser = None

    # --------------------------- init for start service ----------------------------
    def init_for_start_service(self):
        self._save_resources(list(execute_task.resources))
        self._save_task_file(execute_task.task_file)
        self._init_input_receiver(input_endpoints)
        self._init_output_dispenser(output_endpoints)

    def _save_resources(self, resources: List[serializator.SerializableFile]) -> None:
        # _tmp/tm/<task_manager_name>/<subtask_id>/resources/<resource_filename>
        dirpath = "_tmp/tm/{}/{}/resources".format(self.tm_name, self.subtask_id)
        for f in resources:
            f.persistence_to_localfs(dirpath)

    def _save_task_file(self, task_file: serializator.SerializableFile) -> None:
        # _tmp/tm/<task_manager_name>/<subtask_id>/taskfile/<task_filename>
        dirpath = "_tmp/tm/{}/{}/taskfile".format(self.tm_name, self.subtask_id)
        task_file.persistence_to_localfs(dirpath)

    def _init_output_dispenser(self, output_endpoints: List[str]):
        self.input_channel = multiprocessing.Queue()
        self.output_dispenser = OutputDispenser(output_endpoints)

    def _init_input_receiver(self, input_endpoints: List[str]):
        self.input_receiver = InputReceiver(input_endpoints)

    # --------------------------- start ----------------------------
    def start(self):
        pass


