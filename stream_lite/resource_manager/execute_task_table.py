#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import copy

from readerwriterlock import rwlock
from typing import List, Dict, Union
from stream_lite.proto import resource_manager_pb2
from stream_lite.job_manager import scheduler


class ExecuteTaskInfo(object):

    def __init__(self, subtask_name: str, 
            upstream_cls_names: List[str],
            downstream_cls_names: List[str],
            cls_name: str, partition_idx: int, 
            task_manager_name: str):
        self.subtask_name = subtask_name
        self.upstream_cls_names = upstream_cls_names
        self.downstream_cls_names = downstream_cls_names
        self.cls_name = cls_name
        self.partition_idx = partition_idx
        self.task_manager_name = task_manager_name
        # defined in schduler
        self.currency = int(subtask_name.split("/")[1][:-1])

    def is_source(self):
        return len(self.upstream_cls_names) == 0

    def is_sink(self):
        return len(self.downstream_cls_names) == 0

    def __str__(self):
        return "subtask_name: {}\n".format(self.subtask_name) +\
                "upstream_cls_names: {}\n".format(self.upstream_cls_names) +\
                "downstream_cls_names: {}\n".format(self.downstream_cls_names) +\
                "cls_name: {}\n".format(self.cls_name) +\
                "partition_idx: {}\n".format(self.partition_idx) +\
                "task_manager_name: {}\n".format(self.task_manager_name) +\
                "currency: {}\n".format(self.currency)


class ExecuteTaskTable(object):

    def __init__(self):
        self.rw_lock_pair = rwlock.RWLockFair()
        self.exec_task_infos = {} # subtask_name: info

    def build_from_rpc_request(self, request: resource_manager_pb2.RegisterJobExecuteInfoRequest):
        task_manager_names = request.task_manager_names
        exec_tasks = request.exec_tasks

        for idx, task_manager_name in enumerate(task_manager_names):
            exec_task = exec_tasks[idx]
            
            upstream_currency = len(exec_task.input_endpoints)
            upstream_cls_names = []
            if upstream_currency != 0:
                assert len(exec_task.upstream_cls_names) == 1, "except 1 but get {}".format(
                        len(exec_task.upstream_cls_names))
                upstream_cls_names = [
                        scheduler.Scheduler._get_subtask_name(
                            exec_task.upstream_cls_names[0],
                            upstream_idx, upstream_currency) for 
                        upstream_idx in range(upstream_currency)]

            downstream_currency = len(exec_task.output_endpoints)
            downstream_cls_names = []
            if downstream_currency != 0:
                assert len(exec_task.downstream_cls_names) == 1, "except 1 but get {}".format(
                        len(exec_task.downstream_cls_names))
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
                        upstream_cls_names=upstream_cls_names,
                        downstream_cls_names=downstream_cls_names,
                        cls_name=cls_name,
                        partition_idx=partition_idx,
                        task_manager_name=task_manager_name))

        #for name, info in self.exec_task_infos.items():
        #    print("[{}]: {}".format(name, info))

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

    def get_info(self, name: str) -> ExecuteTaskInfo:
        with self.rw_lock_pair.gen_rlock():
            if name not in self.exec_task_infos:
                raise KeyError(
                       "Failed to get info: {} does not exist".format(name))
            return self.exec_task_infos[name]

    def get_infos(self) -> Dict[str, ExecuteTaskInfo]:
        with self.rw_lock_pair.gen_rlock():
            return copy.deepcopy(self.exec_task_infos)
