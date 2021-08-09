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
            partition_idx: int, record: common_pb2.Record) -> None:
        #  print(str(record))
        req = subtask_pb2.PushRecordRequest(
                from_subtask=from_subtask,
                partition_idx=partition_idx,
                record=record)
        #  _LOGGER.debug("pushRecord: {}".format(str(req)))
        resp = self.stub.pushRecord(req)
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)

    def triggerCheckpoint(self, 
            checkpoint_id: int, 
            cancel_job: bool,
            migrate_cls_name: str,
            migrate_partition_idx: int) -> None:
        resp = self.stub.triggerCheckpoint(
                subtask_pb2.TriggerCheckpointRequest(
                    checkpoint=common_pb2.Record.Checkpoint(
                        id=checkpoint_id,
                        cancel_job=cancel_job,
                        migrate_cls_name=migrate_cls_name,
                        migrate_partition_idx=migrate_partition_idx)))
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)

    def triggerMigrate(self,
            migrate_id: int, 
            new_cls_name: str, 
            new_partition_idx: int, 
            new_endpoint: str) -> None:
        resp = self.stub.triggerMigrate(
                subtask_pb2.TriggerMigrateRequest(
                    migrate=common_pb2.Record.Migrate(
                        id=migrate_id,
                        new_cls_name=new_cls_name,
                        new_partition_idx=new_partition_idx,
                        new_endpoint=new_endpoint)))
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)

    def terminateSubtask(self,
            terminate_id: int, 
            cls_name: str,
            partition_idx: int, 
            subtask_name: str) -> None:
        resp = self.stub.terminateSubtask(
                subtask_pb2.TerminateSubtaskRequest(
                    terminate_subtask=common_pb2.Record.TerminateSubtask(
                        id=terminate_id,
                        cls_name=cls_name,
                        partition_idx=partition_idx,
                        subtask_name=subtask_name)))
        if resp.status.err_code != 0:
            raise SystemExit(resp.status.message)
