#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
import stream_lite.proto.common_pb2 as common_pb2
from stream_lite.network import serializator
from stream_lite.utils import util
from .client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class JobManagerClient(ClientBase):

    def __init__(self):
        super(JobManagerClient, self).__init__()

    def _init_stub(self, channel):
        return job_manager_pb2_grpc.JobManagerServiceStub(channel)

    def triggerCheckpoint(self, 
            jobid: str, 
            cancel_job: bool = False,
            migrate_cls_name: str = "",
            migrate_partition_idx: int = -1) -> int:
        resp = self.stub.triggerCheckpoint(
                job_manager_pb2.TriggerCheckpointRequest(
                    jobid=jobid,
                    cancel_job=cancel_job,
                    migrate_cls_name=migrate_cls_name,
                    migrate_partition_idx=migrate_partition_idx))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        '''
        _LOGGER.info(
                "Success to checkpoint (jobid={}, chk_id={})"
                .format(jobid, resp.checkpoint_id))
        '''
        return resp.checkpoint_id

    def acknowledgeCheckpoint(self, 
            subtask_name: str, 
            jobid: str,
            checkpoint_id: int, 
            state: serializator.SerializableFile,
            err_code: int = 0, 
            err_msg: str = "") -> None:
        resp = self.stub.acknowledgeCheckpoint(
                job_manager_pb2.AcknowledgeCheckpointRequest(
                    status=common_pb2.Status(
                        err_code=err_code,
                        message=err_msg),
                    subtask_name=subtask_name,
                    jobid=jobid,
                    checkpoint_id=checkpoint_id,
                    state=state.instance_to_proto()))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def acknowledgeMigrate(self, 
            subtask_name: str, 
            jobid: str,
            migrate_id: int, 
            err_code: int = 0, 
            err_msg: str = "") -> None:
        resp = self.stub.acknowledgeMigrate(
                job_manager_pb2.AcknowledgeMigrateRequest(
                    status=common_pb2.Status(
                        err_code=err_code,
                        message=err_msg),
                    subtask_name=subtask_name,
                    jobid=jobid,
                    migrate_id=migrate_id))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def acknowledgeTerminate(self, 
            subtask_name: str, 
            jobid: str,
            terminate_id: int, 
            err_code: int = 0, 
            err_msg: str = "") -> None:
        resp = self.stub.acknowledgeTerminate(
                job_manager_pb2.AcknowledgeTerminateRequest(
                    status=common_pb2.Status(
                        err_code=err_code,
                        message=err_msg),
                    subtask_name=subtask_name,
                    jobid=jobid,
                    terminate_id=terminate_id))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def notifyMigrateSynchron(self,
            jobid: str,
            migrate_id: int) -> None:
        resp = self.stub.notifyMigrateSynchron(
                job_manager_pb2.NotifyMigrateSynchronRequest(
                    jobid=jobid, migrate_id=migrate_id))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def triggerMigrate(self, 
            jobid: str,
            src_cls_name: str,
            src_partition_idx: int,
            src_currency: int,
            target_task_manager_locate: str,
            with_checkpoint_id: int = -1) -> None:
        resp = self.stub.triggerMigrate(
                job_manager_pb2.MigrateRequest(
                    jobid=jobid,
                    src_cls_name=src_cls_name,
                    src_partition_idx=src_partition_idx,
                    src_currency=src_currency,
                    target_task_manager_locate=target_task_manager_locate,
                    with_checkpoint_id=with_checkpoint_id))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success to migrate job(jobid={})".format(jobid))
