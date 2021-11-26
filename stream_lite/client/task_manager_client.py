#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time
from typing import List, Dict, Union, Optional

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
from stream_lite.proto import task_manager_pb2, task_manager_pb2_grpc
from stream_lite.proto import common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class TaskManagerClient(ClientBase):

    def __init__(self):
        super(TaskManagerClient, self).__init__()

    def _init_stub(self, channel):
        return task_manager_pb2_grpc.TaskManagerServiceStub(channel)

    def requestSlot(self, seri_tasks: List[serializator.SerializableTask]) -> List[int]:
        slot_desc_protos = []
        for task in seri_tasks:
            slot_desc_protos.extend(
                    [serializator.SerializableRequiredSlotDesc.to_proto()
                        for i in range(task.currency)])

        resp = self.stub.requestSlot(
                task_manager_pb2.RequiredSlotRequest(
                    slot_descs=slot_desc_protos))

        if resp.status.err_code != 0:
            raise RuntimeError(resp.status.message)

        return list(resp.available_ports)

    def deployTask(self, jobid: str,
            exec_task: serializator.SerializableExectueTask,
            checkpoint_id: int,
            state_file: Union[None, common_pb2.File]) -> None:
        req = task_manager_pb2.DeployTaskRequest(
                exec_task=exec_task.instance_to_proto(),
                jobid=jobid)
        if state_file is not None:
            # job manager 发送状态文件
            req.state.CopyFrom(
                    task_manager_pb2.DeployTaskRequest.State(
                        type="FILE",
                        state_file=state_file))
        elif checkpoint_id >= 0:
            # task manager 从本地查找对应的状态文件
            req.state.CopyFrom(
                    task_manager_pb2.DeployTaskRequest.State(
                        type="LOCAL",
                        local_checkpoint_id=checkpoint_id))
        else:
            # 没有状态文件
            pass
        resp = self.stub.deployTask(req)
        
        if resp.status.err_code != 0:
            raise RuntimeError(resp.status.message)

    def startTask(self, subtask_name: str):
        resp = self.stub.startTask(
                task_manager_pb2.StartTaskRequest(
                    subtask_name=subtask_name))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def testLatency(self) -> int:
        s_timestamp = util.get_timestamp()
        resp = self.stub.testLatency(
                task_manager_pb2.TestLatencyRequest(
                    timestamp=s_timestamp))
        latency = resp.latency
        e_timestamp = util.get_timestamp()
        latency = e_timestamp - s_timestamp
        
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

        return latency

    def preCopyState(self,
            jobid: str,
            checkpoint_id: int,
            state_file: common_pb2.File,
            cls_name: str,
            partition_idx: int) -> None:
        resp = self.stub.preCopyState(
                task_manager_pb2.PreCopyStateRequest(
                    jobid=jobid,
                    checkpoint_id=checkpoint_id,
                    state_file=state_file,
                    cls_name=cls_name,
                    partition_idx=partition_idx))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def resetSlotTable(self):
        resp = self.stub.resetSlotTable(common_pb2.NilRequest())
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
