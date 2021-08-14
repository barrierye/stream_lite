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

    def registerTaskManager(self, endpoint: str, conf: dict) -> None:
        job_manager_enpoint = conf["job_manager_enpoint"]
        resp = self.stub.registerTaskManager(
                job_manager_pb2.RegisterTaskManagerRequest(
                    task_manager_desc=serializator.SerializableTaskManagerDesc.to_proto(
                        host=endpoint.split(":")[0],
                        endpoint=endpoint,
                        name=conf["name"],
                        coord_x=conf["coord"]["x"],
                        coord_y=conf["coord"]["y"],
                        resource=conf["resource"])))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)
        _LOGGER.info(
                "Success register task manager(name={})".format(conf["name"]) +\
                " to job manager(endpoint={})".format(job_manager_enpoint))

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
