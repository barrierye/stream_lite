#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import os
import grpc
import logging
import yaml
import copy
import time

import stream_lite.proto.task_manager_pb2_grpc as task_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.proto import task_manager_pb2

import stream_lite
from stream_lite.utils import util
from stream_lite.client import JobManagerClient, ResourceManagerClient
from stream_lite.network import serializator
from stream_lite.network.util import gen_nil_response
import stream_lite.utils.util
from stream_lite.utils import AvailablePortGenerator

from .slot_table import SlotTable
from .heart_beat_helper import HeartBeatHelper

_LOGGER = logging.getLogger(__name__)


class TaskManagerServicer(task_manager_pb2_grpc.TaskManagerServiceServicer):

    def __init__(self, rpc_port: int, conf_yaml_path: str):
        super(TaskManagerServicer, self).__init__()
        self.resource_manager_client = None
        self.endpoint = "{}:{}".format(
                stream_lite.utils.util.get_ip(), rpc_port)
        self.conf = self._init_by_yaml(conf_yaml_path)
        job_manager_enpoint, resource_manager_enpoint = self._register(self.conf)
        self.slot_table = SlotTable(
                self.name, job_manager_enpoint,
                self.resource.slot_number)
        
        self.heart_beat_helper = HeartBeatHelper(
                self.name,
                self.endpoint,
                self.coord,
                resource_manager_enpoint)
        _LOGGER.info("[{}] Start HeartbeatHelper".format(self.name))
        self.heart_beat_helper.run_on_standalone_process(stream_lite.config.IS_PROCESS)
        # jobid, cls_name, partition_idx
        self.snapshot_dir = "_tmp/tm/{}".format(self.name) + "/jobid_{}/{}/partition_{}/snapshot"

    def _init_by_yaml(self, conf_yaml_path: str) -> dict:
        with open(conf_yaml_path) as f:
            conf = yaml.load(f.read(), Loader=yaml.Loader)
        
        self.name = conf["name"]
        self.coord = common_pb2.Coordinate(
                x=conf["coord"]["x"], 
                y=conf["coord"]["y"])
        self.resource = serializator.SerializableMachineResource(
                slot_number=conf["resource"]["slot_number"])
        self.remain_resource = copy.deepcopy(self.resource)
        return conf

    def _register(self, conf: dict) -> (str, str):
        resource_manager_enpoint = conf["resource_manager_enpoint"]
        self.resource_manager_client = ResourceManagerClient()
        _LOGGER.debug(
                "Try connect to resource manager({}) from task manager(name={})"
                .format(resource_manager_enpoint, conf["name"]))
        self.resource_manager_client.connect(resource_manager_enpoint)
        while True:
            try:
                job_manager_enpoint = self.resource_manager_client.registerTaskManager(
                        self.endpoint, self.conf)
            except grpc._channel._InactiveRpcError as e:
                _LOGGER.debug(
                    "Failed to register task manager: connections to resource manager"
                    "failing, waiting for 5 sec...")
                time.sleep(5)
                continue
            break
        return job_manager_enpoint, resource_manager_enpoint

    # --------------------------- request slot ----------------------------
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

    # --------------------------- deploy task ----------------------------
    def deployTask(self, request, context):
        if self.remain_resource.slot_number <= 0:
            err_msg = "Failed: available slot not enough"
            _LOGGER.error(err_msg)
            return gen_nil_response(
                    err_code=1, message=err_msg)
        try:
            self.slot_table.deployExecuteTask(
                    request.jobid, 
                    request.exec_task, 
                    request.state)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=str(e))
        self.remain_resource.slot_number -= 1
        return gen_nil_response()

    # --------------------------- start task ----------------------------
    def startTask(self, request, context):
        subtask_name = request.subtask_name
        try:
            self.slot_table.startExecuteTask(subtask_name)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=str(e))
        return gen_nil_response()
    
    # --------------------------- test latency ----------------------------
    def testLatency(self, request, context):
        current_timestamp = util.get_timestamp()
        req_timestamp = request.timestamp
        latency = current_timestamp - req_timestamp
        return task_manager_pb2.TestLatencyResponse(
                latency=latency,
                status=common_pb2.Status())
    
    # --------------------------- pre copy state ----------------------------
    def preCopyState(self, request, context):
        try:
            jobid = request.jobid
            checkpoint_id = request.checkpoint_id
            cls_name = request.cls_name
            partition_idx = request.partition_idx
            
            state_file = serializator.SerializableFile.from_proto(
                    request.state_file)
            # jobid, cls_name, partition_idx
            chk_prefix_path = self.snapshot_dir.format(
                    jobid, cls_name, partition_idx)
            state_file.persistence_to_localfs(chk_prefix_path)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=str(e))
        return gen_nil_response()
