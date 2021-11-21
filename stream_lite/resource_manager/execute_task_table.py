#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
from readerwriterlock import rwlock
from typing import List, Dict, Union
from stream_lite.proto import resource_manager_pb2
from stream_lite.job_manager import scheduler


class ExecuteTaskTable(object):

    def __init__(self):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.exec_task_infos = {} # subtask_name: info

    def build_from_rpc_request(self, request: resource_manager_pb2.RegisterJobExecuteInfoRequest):
        task_manager_names = request.task_manager_names
        exec_tasks = request.exec_tasks

        for idx, task_manager_name in enumerate(task_manager_names):
            exec_task = exec_tasks[idx]
            
            downstream_currency = len(exec_task.output_endpoints)
            assert len(exec_task.downstream_cls_names) == 1
            downstream_cls_names = [
                    scheduler.Scheduler._get_subtask_name(
                        exec_task.downstream_cls_names[0],
                        downstream_idx, downstream_currency) for 
                    downstream_idx in range(downstream_currency)]

            subtask_name = exec_task.subtask_name
            cls_name = exec_task.cls_name
            partition_idx = exec_task.partition_idx
            self._add_exec_task_info(
                    subtask_name=subtask_name,
                    info=ExecuteTaskInfo(
                        subtask_name=subtask_name,
                        downstream_cls_names=downstream_cls_names,
                        cls_name=cls_name,
                        partition_idx=partition_idx,
                        task_manager_name=task_manager_name))

    def _add_exec_task_info(self, subtask_name: str,
            info: ExecuteTaskInfo) -> None:
        with self.rw_lock_pair.gen_wlock():
            if subtask_name in self.exec_task_infos:
                raise KeyError(
                        "Failed to add exec task info: {} already exists".format(
                            subtask_name))
            self.exec_task_infos[subtask_name] = info
        
    def update_exec_task_info(self, subtask_name: str,
            info: ExecuteTaskInfo) -> None:
        with self.rw_lock_pair.gen_wlock():
            self.exec_task_infos[subtask_name] = info

    def get_info(self, name: str) -> ExecuteTaskTable:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.exec_task_infos:
                raise KeyError(
                       "Failed to get info: {} does not exist".format(name))
            return self.exec_task_infos[name]


class ExecuteTaskInfo(object):

    def __init__(self, subtask_name: str, downstream_cls_names: List[str],
            cls_name: str, partition_idx: int, task_manager_name: str):
        self.subtask_name = subtask_name
        self.downstream_cls_names = downstream_cls_names
        self.cls_name = cls_name
        self.partition_idx = partition_idx
        self.task_manager_name = task_manager_name
