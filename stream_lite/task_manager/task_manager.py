#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import grpc
import logging
import yaml
import copy
import time

import stream_lite.proto.task_manager_pb2_grpc as task_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.proto import task_manager_pb2

from stream_lite.client import JobManagerClient
from stream_lite.network import serializator
import stream_lite.utils.util
from stream_lite.utils import AvailablePortGenerator

_LOGGER = logging.getLogger(__name__)


class TaskManagerServicer(task_manager_pb2_grpc.TaskManagerServiceServicer):

    def __init__(self, rpc_port: int, conf_yaml_path: str):
        super(TaskManagerServicer, self).__init__()
        self.job_manager_client = None
        self.endpoint = "{}:{}".format(
                stream_lite.utils.util.get_ip(), rpc_port)
        self.conf = self._init_by_yaml(conf_yaml_path)
        self._register(self.conf)

    def _init_by_yaml(self, conf_yaml_path: str) -> dict:
        with open(conf_yaml_path) as f:
            conf = yaml.load(f.read(), Loader=yaml.Loader)
        
        self.name = conf["name"]
        self.coord = (conf["coord"]["x"], conf["coord"]["y"])
        self.resource = serializator.SerializableMachineResource(
                slot_number=conf["resource"]["slot_number"])
        self.remain_resource = copy.deepcopy(self.resource)
        return conf

    def _register(self, conf: dict):
        job_manager_enpoint = conf["job_manager_enpoint"]
        self.job_manager_client = JobManagerClient()
        _LOGGER.debug(
                "Try connect to job manager({}) from task manager(name={})"
                .format(job_manager_enpoint, conf["name"]))
        self.job_manager_client.connect(job_manager_enpoint)
        while True:
            try:
                self.job_manager_client.registerTaskManager(
                        self.endpoint, self.conf)
            except grpc._channel._InactiveRpcError as e:
                _LOGGER.debug(
                    "Failed to register task manager: connections to job manager"
                    "failing, waiting for 5 sec...")
                time.sleep(5)
                continue
            break

    def requestSlot(self, request, context):
        slot_descs = [serializator.SerializableRequiredSlotDesc.from_proto(p)
                for p in request.slot_descs]

        # TODO: 这里只检查了请求的 slot 个数
        if len(slot_descs) > self.remain_resource.slot_number:
            err_msg = "Failed: available slot not enough ({} < {})".format(
                    self.remain_resource.slot_number, len(slot_descs))
            _LOGGER.error(err_msg)
            return task_manager_pb2.RequiredSlotResponse(
                    status=common_pb2.Status(
                        err_code=1, 
                        message=err_msg))
    
        # 找可用的端口
        available_ports = [AvailablePortGenerator().next()
            for i in range(len(slot_descs))]
        return task_manager_pb2.RequiredSlotResponse(
            status=common_pb2.Status(),
            available_ports=available_ports)

    def deployTask(self, request, context):
        pass

    
