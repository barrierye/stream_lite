#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import grpc
import logging
import pickle
import inspect
from typing import List, Dict, Tuple, Set

import stream_lite.proto.job_manager_pb2 as job_manager_pb2
import stream_lite.proto.common_pb2 as common_pb2
import stream_lite.proto.job_manager_pb2_grpc as job_manager_pb2_grpc
from stream_lite.network.util import gen_nil_response
from stream_lite.network import serializator
from . import scheduler

from .registered_task_manager_table import RegisteredTaskManagerTable

_LOGGER = logging.getLogger(__name__)


class JobManagerServicer(job_manager_pb2_grpc.JobManagerServiceServicer):

    def __init__(self):
        super(JobManagerServicer, self).__init__()
        self.registered_task_manager_table = RegisteredTaskManagerTable()
        self.scheduler = scheduler.UserDefinedScheduler(
                self.registered_task_manager_table)

    # --------------------------- submit job ----------------------------
    def submitJob(self, request, context):
        persistence_dir = "./_tmp/server/task_files"
        seri_tasks = []
        for task in request.tasks:
            seri_task = serializator.SerializableTask.from_proto(task)
            seri_task.task_file.persistence_to_localfs(persistence_dir)
            seri_tasks.append(seri_task)
        
        try:
            resp = self._innerSubmitJob(seri_tasks)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(err_code=1, message=e)
        return resp 
        
    def _innerSubmitJob(self, seri_tasks):
        # schedule
        execute_map = self.scheduler.schedule(seri_tasks)
        
        # deploy
        self._deployExecuteTasks(execute_map)
        _LOGGER.info("Success deploy all task.")

        # start
        self._startExecuteTasks(execute_map)
        _LOGGER.info("Success start all task.")

        return gen_nil_response()
 
    def _deployExecuteTasks(self, 
            execute_map: Dict[str, List[serializator.SerializableExectueTask]]):
        for task_manager_name, seri_execute_tasks in execute_map.items():
            client = self.registered_task_manager_table.get_client(task_manager_name)
            for execute_task in seri_execute_tasks:
                resp = client.deployTask(
                        task_manager_pb2.DeployTaskRequest(
                            exec_task=execute_task.to_proto()))
                if resp.status.err_code != 0:
                    raise Exception(resp.status.message)

    def _startExecuteTasks(self, 
            logical_map: Dict[str, List[serializator.SerializableTask]],
            execute_map: Dict[str, List[serializator.SerializableExectueTask]]):
        """
        从末尾往前 start, 确保 output 的 rpc 服务已经起来
        """
        # cls_name -> serializator.SerializableTask
        task_name_inverted_index = {}
        for seri_tasks in logical_map.values():
            for seri_task in seri_tasks:
                if seri_task not in task_name_inverted_index:
                    task_name_inverted_index[seri_task.cls_name] = seri_task

        # cls_name -> output_task: List[str]
        next_logical_tasks = {}
        for seri_tasks in logical_map.values():
            for seri_task in seri_tasks:
                for pre_task in seri_task.input_tasks:
                    if pre_task not in next_logical_tasks:
                        next_logical_tasks[pre_task] = []
                    next_logical_tasks[pre_task].append(seri_task.cls_name)

        # subtask_name -> (task_manager_name, serializator.SerializableExecuteTask)
        subtask_name_inverted_index = {}
        for task_manager_name, exec_task in execute_map.items():
            subtask_name_inverted_index[exec_task.subtask_name] = \
                    (subtask_name_inverted_index, exec_task)

        started_tasks = set()
        for cls_name in subtask_name_inverted_index.keys():
            if cls_name not in started_tasks:
                self._dfsToStartExecuteTask(
                        cls_name, 
                        next_logical_tasks,
                        task_name_inverted_index, 
                        subtask_name_inverted_index,
                        started_tasks)

    def _dfsToStartExecuteTask(self, 
            cls_name: str,
            next_logical_tasks: Dict[str, List[str]],
            logicaltask_name_inverted_index: Dict[str, serializator.SerializableTask],
            subtask_name_inverted_index: Dict[str, Tuple[str, serializator.SerializableExectueTask]],
            started_tasks: Set[str]):
        if cls_name in started_tasks:
            return
        started_tasks.add(cls_name)
        for next_logical_task_name in next_logical_tasks[cls_name]:
            if next_logical_task_name not in started_tasks:
                self._dfsToStartExecuteTask(
                        next_logical_task_name,
                        next_logical_tasks,
                        logicaltask_name_inverted_index,
                        subtask_name_inverted_index,
                        started_tasks)
        logical_task = logicaltask_name_inverted_index[cls_name]
        for i in range(logical_task.currency):
            subtask_name = self._get_subtask_name(
                    cls_name, i, logical_task.currency)
            execute_task = subtask_name_inverted_index[subtask_name]
            self._innerDfsToStartExecuteTask(
                    task_manager_name,
                    execute_task)

    def _innerDfsToStartExecuteTask(self, 
            task_manager_name: str,
            execute_task: serializator.SerializableExectueTask):
        client = self.registered_task_manager_table.get_client(task_manager_name)
        resp = client.startTask(task_manager_pb2.StartTaskRequest(
            subtask_name=execute_task.subtask_name))
        if resp.status.err_code != 0:
            raise Exception(resp.status.message)

    def _get_subtask_name(self, cls_name: str, idx: int, currency: int) -> str:
        return "{}#({}/{})".format(cls_name, idx, currency)

    # --------------------------- register task manager ----------------------------
    def registerTaskManager(self, request, context):
        try:
            self.registered_task_manager_table.register(
                    request.task_manager_desc)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=e)
        return gen_nil_response()

