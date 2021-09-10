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

_LOGGER = logging.getLogger(__name__)


class CheckpointHelper(object):
    """
    工具类（仅被 job_manager 使用）: 周期性地checkpoint
    """

    def __init__(self,
            jobid: str,
            job_manager_enpoint: str,
            interval: int = 5,
            ):
        self.jobid = jobid
        self.job_manager_endpoint = job_manager_enpoint
        self.interval = interval
        self._process = None

    def _inner_run(self, 
            jobid: str,
            job_manager_endpoint: str) -> None:
        # init client
        client = JobManagerClient()
        client.connect(job_manager_endpoint)
        while True:
            time.sleep(self.interval)
            client.triggerCheckpoint(
                    jobid=jobid, 
                    cancel_job=False)

    def run(self, 
            jobid: str,
            job_manager_endpoint: str) -> None:
        try:
            self._inner_run(jobid, job_manager_endpoint)
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
                    args=(
                        self.jobid,
                        self.job_manager_endpoint),
                    daemon=False)
        else:
            self._process = threading.Thread(
                    target=self.run,
                    args=(
                        self.jobid,
                        self.job_manager_endpoint))
        self._process.start()
