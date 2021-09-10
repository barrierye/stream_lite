#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import grpc
import logging
import pickle
import inspect
import time
import yaml

from stream_lite.proto import job_manager_pb2, job_manager_pb2_grpc
from stream_lite.network import serializator
from stream_lite.utils import util
from stream_lite.client.client_base import ClientBase

_LOGGER = logging.getLogger(__name__)


class UserClient(ClientBase):

    def __init__(self):
        super(UserClient, self).__init__()

    def _init_stub(self, channel):
        return job_manager_pb2_grpc.JobManagerServiceStub(channel)

    def submitJob(self, 
            yaml_path: str, 
            periodicity_checkpoint_interval_s: float,
            auto_migrate: bool = False) -> str:
        with open(yaml_path) as f:
            conf = yaml.load(f.read(), Loader=yaml.FullLoader)

        seri_tasks = []
        for task_dict in conf["tasks"]:
            seri_tasks.append(
                    serializator.SerializableTask.to_proto(
                        task_dict, conf["task_files_dir"]))

        req = job_manager_pb2.SubmitJobRequest(
                tasks=seri_tasks,
                periodicity_checkpoint_interval_s=periodicity_checkpoint_interval_s,
                auto_migrate=auto_migrate)
        resp = self.stub.submitJob(req)
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info("Success to submit job (jobid={})".format(resp.jobid))
        return resp.jobid
   
    def restoreFromCheckpoint(self, 
            jobid: str, 
            checkpoint_id: int) -> str:
        resp = self.stub.restoreFromCheckpoint(
                job_manager_pb2.RestoreFromCheckpointRequest(
                    checkpoint_id=checkpoint_id,
                    jobid=jobid))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success to restore from checkpoint (chk_id={}), jobid={}"
                .format(checkpoint_id, resp.jobid))
        return resp.jobid

    def triggerMigrate(self, 
            jobid: str,
            src_cls_name: str,
            src_partition_idx: int,
            src_currency: int,
            target_task_manager_locate: str) -> None:
        resp = self.stub.triggerMigrate(
                job_manager_pb2.MigrateRequest(
                    jobid=jobid,
                    src_cls_name=src_cls_name,
                    src_partition_idx=src_partition_idx,
                    src_currency=src_currency,
                    target_task_manager_locate=target_task_manager_locate))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success to migrate job(jobid={})".format(jobid))
