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
            state: Union[None, common_pb2.File]) -> None:
        req = task_manager_pb2.DeployTaskRequest(
                exec_task=exec_task.instance_to_proto(),
                jobid=jobid)
        if state is not None:
            req.state.CopyFrom(state)
        resp = self.stub.deployTask(req)
        
        if resp.status.err_code != 0:
            raise RuntimeError(resp.status.message)

    def startTask(self, subtask_name: str):
        resp = self.stub.startTask(
                task_manager_pb2.StartTaskRequest(
                    subtask_name=subtask_name))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
