#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
from concurrent import futures
import os
import grpc
import logging
import pickle
import inspect
from typing import List, Dict, Tuple, Set

import stream_lite.proto.job_manager_pb2 as job_manager_pb2
import stream_lite.proto.common_pb2 as common_pb2
import stream_lite.proto.job_manager_pb2_grpc as job_manager_pb2_grpc

from stream_lite import client
from stream_lite.network.util import gen_nil_response
from stream_lite.network import serializator
from stream_lite.utils import JobIdGenerator, EventIdGenerator

from . import scheduler
from .registered_task_manager_table import RegisteredTaskManagerTable
from .job_coordinator import JobCoordinator

_LOGGER = logging.getLogger(__name__)


class JobManagerServicer(job_manager_pb2_grpc.JobManagerServiceServicer):

    def __init__(self):
        super(JobManagerServicer, self).__init__()
        self.registered_task_manager_table = RegisteredTaskManagerTable()
        self.scheduler = scheduler.UserDefinedScheduler(
                self.registered_task_manager_table)
        self.job_coordinator = JobCoordinator(
                self.registered_task_manager_table)
        self.jobinfo_dir = "./_tmp/jm/jobid_{}" # jobid
        self.taskfile_dir = "./_tmp/jm/jobid_{}/taskfiles"   # jobid
        self.resource_dir = "./_tmp/jm/jobid_{}/resource/{}" # jobid, cls_name
        self.snapshot_dir = "./_tmp/jm/jobid_{}/snapshot/{}/partition_{}" # jobid, cls_name, part
        self.exetasks_dir = "./_tmp/jm/jobid_{}/physical/{}/partition_{}" # jobid, cls_name, part

    # --------------------------- submit job ----------------------------
    def submitJob(self, request, context):
        jobid = JobIdGenerator().next()

        # persistence_to_localfs: whole request
        jobinfo_path = self.jobinfo_dir.format(jobid)
        os.system("mkdir -p {}".format(jobinfo_path))
        with open(os.path.join(
            jobinfo_path, "jobinfo.prototxt"), "wb") as f:
            f.write(request.SerializeToString())
        
        seri_tasks = []
        for task in request.tasks:
            seri_task = serializator.SerializableTask.from_proto(task)
            seri_tasks.append(seri_task)
        
        try:
            execute_map = self._innerSubmitJob(seri_tasks, jobid)

            # persistence_to_localfs: physical task
            for task_manager_name, execute_tasks in execute_map.items():
                for task in execute_tasks:
                    task_path = self.exetasks_dir.format(
                            jobid, task.cls_name, task.partition_idx)
                    os.system("mkdir -p {}".format(task_path))
                    with open(os.path.join(
                        task_path, "task.prototxt"), "wb") as f:
                        f.write(task.instance_to_proto().SerializeToString())

            # 把所有 Op 信息注册到 CheckpointCoordinator 里
            self.job_coordinator.register_job(jobid, execute_map)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return job_manager_pb2.SubmitJobResponse(
                    status=common_pb2.Status(
                        err_code=1, message=str(e)))

        return job_manager_pb2.SubmitJobResponse(
                status=common_pb2.Status(),
                jobid=jobid)
        
    def _innerSubmitJob(self, 
            seri_tasks: List[serializator.SerializableTask],
            jobid: str) -> Dict[str, List[serializator.SerializableExectueTask]]:
        # schedule
        logical_map, execute_map = self.scheduler.schedule(seri_tasks)
        
        # deploy
        self._deployExecuteTasks(jobid, execute_map)
        _LOGGER.info("Success deploy all task.")

        # start
        self._startExecuteTasks(logical_map, execute_map)
        _LOGGER.info("Success start all task.")

        return execute_map

    def _deployExecuteTask(self,
            jobid: str,
            client: client.TaskManagerClient,
            execute_task: serializator.SerializableExectueTask,
            with_state: bool = False,
            chk_jobid: str = "", # checkpoint 时候的 jobid
            checkpoint_id: int = -1) -> None:
        state = None
        if with_state:
            cls_name = execute_task.cls_name
            partition_idx = execute_task.partition_idx
            state_name = "chk_{}".format(checkpoint_id)
            snapshot_path = os.path.join(
                    self.snapshot_dir.format(
                        chk_jobid, cls_name, partition_idx),
                    state_name)
            state = serializator.SerializableFile.to_proto(
                    snapshot_path, state_name)
        client.deployTask(jobid, execute_task, state)

    def _deployExecuteTasks(self, 
            jobid: str,
            execute_map: Dict[str, List[serializator.SerializableExectueTask]],
            with_state: bool = False,
            chk_jobid: str = "",
            checkpoint_id: int = -1) -> None:
        for task_manager_name, seri_execute_tasks in execute_map.items():
            client = self.registered_task_manager_table.get_client(task_manager_name)
            for execute_task in seri_execute_tasks:
                self._deployExecuteTask(
                        jobid=jobid,
                        client=client,
                        execute_task=execute_task,
                        with_state=with_state,
                        chk_jobid=chk_jobid,
                        checkpoint_id=checkpoint_id)

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
        for task_manager_name, exec_tasks in execute_map.items():
            for exec_task in exec_tasks:
                subtask_name_inverted_index[exec_task.subtask_name] = \
                        (task_manager_name, exec_task)

        started_tasks = set()
        for cls_name in task_name_inverted_index.keys():
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
        for next_logical_task_name in next_logical_tasks.get(cls_name, []):
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
            task_manager_name, execute_task = subtask_name_inverted_index[subtask_name]
            self._innerStartExecuteTask(
                    task_manager_name, execute_task)

    def _innerStartExecuteTask(self, 
            task_manager_name: str,
            execute_task: serializator.SerializableExectueTask):
        client = self.registered_task_manager_table.get_client(task_manager_name)
        client.startTask(execute_task.subtask_name)

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
                    err_code=1, message=str(e))
        return gen_nil_response()

    # --------------------------- trigger checkpoint ----------------------------
    def triggerCheckpoint(self, request, context):
        try:
            jobid = request.jobid
            checkpoint_id = EventIdGenerator().next()
            self.job_coordinator.trigger_checkpoint(
                    jobid=jobid, 
                    checkpoint_id=checkpoint_id, 
                    cancel_job=request.cancel_job,
                    migrate_cls_name="",
                    migrate_partition_idx=0)
            self.job_coordinator.block_util_checkpoint_completed(
                    jobid=jobid,
                    checkpoint_id=checkpoint_id)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return job_manager_pb2.TriggerCheckpointResponse(
                    status=common_pb2.Status(
                        err_code=1, 
                        message=str(e)))
        return job_manager_pb2.TriggerCheckpointResponse(
                status=common_pb2.Status(),
                checkpoint_id=checkpoint_id)

    # --------------------------- acknowledge checkpoint ----------------------------
    def acknowledgeCheckpoint(self, request, context):
        if request.status.err_code != 0:
            _LOGGER.error(
                    "Failed to acknowledge checkpoint: status.err_code != 0 ({})"
                    .format(request.status.message))
            return
        succ = self.job_coordinator.acknowledgeCheckpoint(request)
        seri_file = serializator.SerializableFile.from_proto(request.state)
        cls_name = request.subtask_name.split("#")[0]
        jobid = request.jobid
        partition_idx = request.subtask_name.split("#")[1].strip("()").split("/")[0]
        seri_file.persistence_to_localfs(
                self.snapshot_dir.format(jobid, cls_name, partition_idx))
        if succ:
            _LOGGER.info(
                    "Success to complete checkpoint(id={}) of job(id={})"
                    .format(request.checkpoint_id, request.jobid))
        return gen_nil_response()

    # --------------------------- restore from checkpoint ----------------------------
    def restoreFromCheckpoint(self, request, context):
        jobid = request.jobid
        checkpoint_id = request.checkpoint_id
        jobinfo_path = os.path.join(
                self.jobinfo_dir.format(jobid), "jobinfo.prototxt")
        if not os.path.exists(jobinfo_path):
            return job_manager_pb2.RestoreFromCheckpointResponse(
                    status=common_pb2.Status(
                        err_code=1, 
                        message="Failed: can not found job(id={})".format(jobid)))

        with open(jobinfo_path, "rb") as f:
            req = job_manager_pb2.SubmitJobRequest()
            req.ParseFromString(f.read())

        new_jobid = JobIdGenerator().next()
        
        seri_tasks = []
        for task in req.tasks:
            seri_task = serializator.SerializableTask.from_proto(task)
            seri_tasks.append(seri_task)
        
        try:
            execute_map = self._innerRestartJob(
                    seri_tasks, jobid, new_jobid, checkpoint_id)
        
            # 把所有 Op 信息注册到 CheckpointCoordinator 里
            self.job_coordinator.register_job(new_jobid, execute_map)
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return job_manager_pb2.RestoreFromCheckpointResponse(
                    status=common_pb2.Status(
                        err_code=1, message=str(e)))

        return job_manager_pb2.RestoreFromCheckpointResponse(
                status=common_pb2.Status(),
                jobid=new_jobid)
 
    def _innerRestartJob(self, 
            seri_tasks: List[serializator.SerializableTask],
            jobid: str,
            new_jobid: str,
            checkpoint_id: int) -> Dict[str, List[serializator.SerializableExectueTask]]:
        # schedule
        logical_map, execute_map = self.scheduler.schedule(seri_tasks)

        # deploy
        self._deployExecuteTasks(new_jobid, execute_map, True, jobid, checkpoint_id)
        _LOGGER.info("Success deploy all task.")

        # start
        self._startExecuteTasks(logical_map, execute_map)
        _LOGGER.info("Success start all task.")

        return execute_map
    
    # --------------------------- migrate ----------------------------
    def triggerMigrate(self, request, context):
        try:
            jobid = request.jobid
            src_cls_name = request.src_cls_name
            src_partition_idx = request.src_partition_idx
            target_task_manager_locate = request.target_task_manager_locate

            # step 1: checkpoint for migrate
            checkpoint_id = EventIdGenerator().next()
            self.job_coordinator.trigger_checkpoint(
                    jobid=jobid, 
                    checkpoint_id=checkpoint_id, 
                    cancel_job=False,
                    migrate_cls_name=src_cls_name,
                    migrate_partition_idx=src_partition_idx)
            self.job_coordinator.block_util_checkpoint_completed(
                    jobid=jobid,
                    checkpoint_id=checkpoint_id)

            # step 2: deploy and start new subtask in target_task_manager_locate
            # step 2.1: find subtask file, resources and snapshot
            task_path = self.exetasks_dir.format(
                    jobid, src_cls_name, src_partition_idx)
            prototxt_path = os.path.join(task_path, "task.prototxt")
            if not os.path.exists(prototxt_path):
                return gen_nil_response(
                        err_code=1, 
                        message="Failed: can not found job(id={})".format(jobid))
            with open(prototxt_path, "rb") as f:
                execute_task_pb = common_pb2.ExecuteTask()
                execute_task_pb.ParseFromString(f.read())

            # step 2.2: ask for slot resource
            if not self.registered_task_manager_table.has_task_manager(target_task_manager_locate):
                return gen_nil_response(
                        err_code=1,
                        message="Failed: can not found task_manager(name={})".format(
                            target_task_manager_locate))
            client = self.registered_task_manager_table.get_client(target_task_manager_locate)
            available_ports = client.requestSlot([
                serializator.SerializableTask(
                        cls_name=execute_task_pb.cls_name,
                        currency=1,
                        locate=target_task_manager_locate,
                        resources=[],
                        task_file=None,
                        input_tasks=None)])
            exe_task = serializator.SerializableExectueTask.from_proto(execute_task_pb)
            # update exe_task params
            exe_task.port = available_ports[0]
            exe_task_endpoint = "{}:{}".format(
                    self.registered_task_manager_table.get_host(target_task_manager_locate),
                    exe_task.port)
            exe_task.subtask_name = "{}@MIGRATE".format(exe_task.subtask_name)

            # step 2.3: deploy subtask (with state)
            client = self.registered_task_manager_table.get_client(target_task_manager_locate)
            self._deployExecuteTask(
                    jobid=jobid,
                    client=client,
                    execute_task=exe_task,
                    with_state=True,
                    chk_jobid=jobid,
                    checkpoint_id=checkpoint_id)
            _LOGGER.info("Success deploy a new task.")

            # step 2.4: start subtask
            self._innerStartExecuteTask(
                    task_manager_name=target_task_manager_locate,
                    execute_task=exe_task)
            _LOGGER.info("Success start the new task.")
 
            # step 3: broadcast migrate event to notify upstream subtask
            #         start send data to the new subtask.
            migrate_id = EventIdGenerator().next()
            self.job_coordinator.trigger_migrate(
                    jobid=jobid, 
                    new_cls_name=src_cls_name, 
                    new_partition_idx=src_partition_idx,
                    new_endpoint=exe_task_endpoint,
                    migrate_id=migrate_id)
            self.job_coordinator.block_util_migrate_completed(
                    jobid=jobid,
                    migrate_id=migrate_id)
 
            # step 4: close old subtask
            # TODO
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return gen_nil_response(
                    err_code=1, message=str(e))
        return gen_nil_response()

    # --------------------------- acknowledge migrate ----------------------------
    def acknowledgeMigrate(self, request, context):
        if request.status.err_code != 0:
            _LOGGER.error(
                    "Failed to acknowledge migrate: status.err_code != 0 ({})"
                    .format(request.status.message))
            return
        succ = self.job_coordinator.acknowledgeMigrate(request)
        if succ:
            _LOGGER.info(
                    "Success to complete migrate(id={}) of job(id={})"
                    .format(request.migrate_id, request.jobid))
        return gen_nil_response()


