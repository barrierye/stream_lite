#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time
from typing import List, Dict

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
from stream_lite.proto import task_manager_pb2, task_manager_pb2_grpc
from stream_lite.proto import subtask_pb2, subtask_pb2_grpc
from stream_lite.proto import common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class SubTaskClient(ClientBase):

    def __init__(self):
        super(SubTaskClient, self).__init__()

    def _init_stub(self, channel):
        return subtask_pb2_grpc.SubTaskServiceStub(channel)

    def pushRecord(self, from_subtask: str,
            partition_idx: int, record: common_pb2.Record):
        #  print(str(record))
        req = subtask_pb2.PushRecordRequest(
                from_subtask=from_subtask,
                partition_idx=partition_idx,
                record=record)
        _LOGGER.debug("pushRecord: {}".format(str(req)))
        resp = self.stub.pushRecord(req)
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)

    def triggerCheckpoint(self, checkpoint_id: int, cancel_job: bool):
        resp = self.stub.triggerCheckpoint(
                subtask_pb2.TriggerCheckpointRequest(
                    checkpoint=common_pb2.Record.Checkpoint(
                        id=checkpoint_id,
                        cancel_job=cancel_job)))
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)
