#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-18
import multiprocessing
import threading
import logging
import time
import os

from stream_lite.utils import util
from stream_lite.client import JobManagerClient
from stream_lite.client import ResourceManagerClient

_LOGGER = logging.getLogger(__name__)


class PeriodicExecutorBase(object):
    """
    工具类基类（仅被 job_manager 使用）
    """

    def __init__(self,
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            interval: int = 5):
        self._process = None
        self._params_dict = {
            "jobid": jobid,
            "job_manager_endpoint": job_manager_endpoint,
            "interval": interval,
            "resource_manager_enpoint": resource_manager_enpoint,
        }

    def _inner_run(self, **kwargs) -> None: 
        raise NotImplementedError("Failed: function not implemented")

    def run(self, **kwargs) -> None:
        try:
            self._inner_run(**kwargs)
        except Exception as e:
            _LOGGER.critical(
                    "Failed to run heartbeat helper ({})".format(e), exc_info=True)
            os._exit(-1)

    def run_on_standalone_process(self, is_process: bool) -> None:
        if self._process is not None:
            raise RuntimeError(
                    "Failed: process already running")
        if is_process:
            # 这里不能将 daemon 设为 True:
            #    AssertionError: daemonic processes are not allowed to have children
            self._process = multiprocessing.Process(
                    target=self.run, 
                    kwargs=self._params_dict,
                    daemon=False)
        else:
            self._process = threading.Thread(
                    target=self.run,
                    kwargs=self._params_dict)
        self._process.start()


class CheckpointHelper(PeriodicExecutorBase):
    """
    工具类（仅被 job_manager 使用）: 周期性地checkpoint并preCopy
    """

    def __init__(self, 
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            snapshot_dir: str,
            interval: int = 5):
        super(CheckpointHelper, self).__init__(
                jobid, job_manager_endpoint, resource_manager_enpoint, interval)
        self._params_dict["snapshot_dir"] = snapshot_dir

    def _inner_run(self, 
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            snapshot_dir: str,
            interval: int) -> None:
        # init job manager client
        job_manager_client = JobManagerClient()
        job_manager_client.connect(job_manager_endpoint)

        # init resource manager client
        resource_manager_client = ResourceManagerClient()
        resource_manager_client.connect(resource_manager_enpoint)
        while True:
            time.sleep(interval)
            
            # checkpoint
            checkpoint_id = job_manager_client.triggerCheckpoint(
                    jobid=jobid, cancel_job=False)
            
            # 获取自动迁移信息
            migrate_infos = resource_manager_client.getAutoMigrateSubtasks(jobid)

            # 逐subtask预备份
            for migrate_info in migrate_infos:
                cls_name = migrate_info.src_cls_name
                target_task_manager_locate = migrate_info.target_task_manager_locate
                jobid = migrate_info.jobid
                currency = migrate_info.src_currency
                partition_idx = migrate_info.src_partition_idx
                client = resource_manager_client.get_client(
                        target_task_manager_locate)

                # jobid, cls_name, part
                # snapshot_dir = "./_tmp/jm/jobid_{}/snapshot/{}/partition_{}"
                file_name = "chk_{}".format(checkpoint_id)
                state_path = snapshot_dir.format(jobid, cls_name, partition_idx)
                state_file = serializator.SerializableFile.to_proto(
                        path=os.path.join(state_path, file_name), name=file_name)

                client.preCopyState(
                        jobid=jobid,
                        checkpoint_id=checkpoint_id,
                        state_file=state_file,
                        cls_name=cls_name,
                        partition_idx=partition_idx)


class MigrateHelper(PeriodicExecutorBase):
    """
    工具类（仅被 job_manager 使用）: 周期性地migrate
    """

    def _inner_run(self,
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            interval: int) -> None:
        # init job manager client
        job_manager_client = JobManagerClient()
        job_manager_client.connect(job_manager_endpoint)
        
        # init resource manager client
        resource_manager_client = ResourceManagerClient()
        resource_manager_client.connect(resource_manager_enpoint)
        while True:
            time.sleep(interval)

            # 获取自动迁移信息
            migrate_infos = resource_manager_client.getAutoMigrateSubtasks(jobid)

            # 逐subtask迁移
            for migrate_info in migrate_infos:
                cls_name = migrate_info.src_cls_name
                target_task_manager_locate = migrate_info.target_task_manager_locate
                jobid = migrate_info.jobid
                currency = migrate_info.src_currency
                partition_idx = migrate_info.src_partition_idx

                job_manager_client.triggerMigrate(
                        jobid=jobid,
                        src_cls_name=cls_name,
                        src_partition_idx=partition_idx,
                        src_currency=currency,
                        target_task_manager_locate=target_task_manager_locate)
