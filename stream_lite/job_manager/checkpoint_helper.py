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
from stream_lite.network import serializator
from stream_lite.utils import StreamingNameGenerator

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
                    "Failed to run helper ({})".format(e), exc_info=True)
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
            migrate_infos, _ = resource_manager_client.getAutoMigrateSubtasks(jobid)

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
            migrate_infos, _ = resource_manager_client.getAutoMigrateSubtasks(jobid)

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

            # 确认迁移完成
            resource_manager_client.doMigrateLastTime()

class PrecopyAndMigrateHelper(PeriodicExecutorBase):
    """
    工具类（仅被 job_manager 使用）: 周期性地checkpoint并preCopy, 然后migrate
    """

    def __init__(self, 
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            snapshot_dir: str,
            interval: int = 5,
            latency_threshold_ms: int = 20):
        super(PrecopyAndMigrateHelper, self).__init__(
                jobid, job_manager_endpoint, resource_manager_enpoint, interval)
        self._params_dict["snapshot_dir"] = snapshot_dir
        self._params_dict["latency_threshold_ms"] = latency_threshold_ms

    def _inner_run(self, 
            jobid: str,
            job_manager_endpoint: str,
            resource_manager_enpoint: str,
            snapshot_dir: str,
            latency_threshold_ms: int,
            interval: int) -> None:
        # init job manager client
        job_manager_client = JobManagerClient()
        job_manager_client.connect(job_manager_endpoint)

        # init resource manager client
        resource_manager_client = ResourceManagerClient()
        resource_manager_client.connect(resource_manager_enpoint)

        streaming_name_generator = StreamingNameGenerator()
        next_streaming_name = None

        while True:
            time.sleep(interval)
            
            # 获取自动迁移信息
            migrate_infos, latency_diff = resource_manager_client.getAutoMigrateSubtasks(jobid)
            if next_streaming_name is None:
                next_streaming_name = streaming_name_generator.next()

            # TODO
            assert len(migrate_infos) <= 1
            migrate_cls_name = migrate_infos[0].src_cls_name if len(migrate_infos) == 1 else ""
            migrate_partition_idx = migrate_infos[0].src_partition_idx if len(migrate_infos) == 1 else -1

            # checkpoint
            checkpoint_id = job_manager_client.triggerCheckpoint(
                    jobid=jobid, cancel_job=False,
                    migrate_cls_name=migrate_cls_name,
                    migrate_partition_idx=migrate_partition_idx,
                    new_streaming_name=next_streaming_name)

            if migrate_infos:
                _LOGGER.info("Doing preCopy...")

            # 逐subtask预备份
            for migrate_info in migrate_infos:
                cls_name = migrate_info.src_cls_name
                target_task_manager_locate = migrate_info.target_task_manager_locate
                jobid = migrate_info.jobid
                currency = migrate_info.src_currency
                partition_idx = migrate_info.src_partition_idx

                # jobid, cls_name, part
                # snapshot_dir = "./_tmp/jm/jobid_{}/snapshot/{}/partition_{}"
                file_name = "chk_{}".format(checkpoint_id)
                state_path = snapshot_dir.format(jobid, cls_name, partition_idx)

                local_full_fn = os.path.join(state_path, file_name)
                endpoint = resource_manager_client.getTaskManagerEndpoint(
                        target_task_manager_locate).split(":")[0]
                remote_home_path = resource_manager_client.getHomePath(target_task_manager_locate)
                remote_path = os.path.join(remote_home_path, 
                        "_tmp/tm/{}".format(target_task_manager_locate) +\
                                "/jobid_{}/{}/partition_{}/snapshot".format(
                                        jobid, cls_name, partition_idx))
                remote_full_fn = os.path.join(remote_path, file_name)
                cmd = 'ssh root@{} "[ -d {} ] && echo ok || mkdir -p {}"'.format(
                        endpoint, remote_path, remote_path)
                os.system(cmd)
                cmd = "scp {} root@{}:{}".format(local_full_fn, endpoint, remote_full_fn)
                print("run {}".format(cmd))
                os.system(cmd)

            if latency_diff > latency_threshold_ms:
                _LOGGER.info("Doing migrate...")

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
                            target_task_manager_locate=target_task_manager_locate,
                            with_checkpoint_id=checkpoint_id)

                # 确认迁移完成
                resource_manager_client.doMigrateLastTime()
                next_streaming_name = None
