#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
"""
Slot 中实际执行的 SubTask
"""
import os
import logging
import importlib
import multiprocessing
import threading
import queue
import pickle
from typing import List, Dict, Any, Tuple, Union

import stream_lite
from stream_lite.proto import subtask_pb2_grpc
from stream_lite.proto import common_pb2
from stream_lite.proto import task_manager_pb2

from stream_lite.network import serializator
from stream_lite.network.util import gen_nil_response
from stream_lite.client import SubTaskClient, JobManagerClient
from stream_lite.utils import util, FinishJobError, DataIdGenerator

from .migrate_filter_window import MigrateFilterWindow
from .input_receiver import InputReceiver
from .output_dispenser import OutputDispenser
from . import operator

_LOGGER = logging.getLogger(__name__)


class SubTaskServicer(subtask_pb2_grpc.SubTaskServiceServicer):
    """
    rpc   == data ==>  InputReceiver (rpc service process & standalone process) 
          == data ==>  channel (multiprocessing.Queue)
          == data ==>  compute core (standalone process)
          == data ==>  channel (multiprocessing.Queue)
          == data ==>  OutputDispenser (standalone process)
          == data ==>  rpc
    """

    def __init__(self, 
            tm_name: str,
            jobid: str,
            job_manager_enpoint: str,
            execute_task: serializator.SerializableExectueTask,
            state: Union[None, task_manager_pb2.DeployTaskRequest.State]):
        super(SubTaskServicer, self).__init__()
        self.tm_name = tm_name
        self.jobid = jobid
        self.job_manager_enpoint = job_manager_enpoint
        self.subtask_id = id(self)
        self.cls_name = execute_task.cls_name
        self.task_filename = execute_task.task_file.name
        self.input_endpoints = execute_task.input_endpoints
        self.output_endpoints = execute_task.output_endpoints
        self.upstream_cls_names = execute_task.upstream_cls_names # for migrate
        self.downstream_cls_names = execute_task.downstream_cls_names  # for migrate
        self.subtask_name = execute_task.subtask_name
        self.partition_idx = execute_task.partition_idx
        self.port = execute_task.port
        self.state = state
        self.resource_dir = "_tmp/tm/{}/jobid_{}/{}/partition_{}/resource".format(
                self.tm_name, self.jobid, self.cls_name, self.partition_idx)
        self.taskfile_dir = "_tmp/tm/{}/jobid_{}/{}/partition_{}/taskfile".format(
                self.tm_name, self.jobid, self.cls_name, self.partition_idx)
        self.snapshot_dir = "_tmp/tm/{}/jobid_{}/{}/partition_{}/snapshot".format(
                self.tm_name, self.jobid, self.cls_name, self.partition_idx)
        self.resource_path_dict = self._save_resources(
                self.resource_dir, list(execute_task.resources))
        if self.cls_name not in operator.BUILDIN_OPS:
            self._save_taskfile(
                    self.taskfile_dir, execute_task.task_file)
        
        self.input_channel = None
        self.output_channel = None
        self.input_receiver = None
        self.output_dispenser = None
        self._core_process = None

    def _save_resources(self, dirpath: str,
            resources: List[serializator.SerializableFile]) -> None:
        resource_path_dict = {}
        for f in resources:
            f.persistence_to_localfs(dirpath)
            resource_path_dict[f.name] = os.path.join(dirpath, f.name)
        return resource_path_dict

    def _save_taskfile(self, dirpath: str,
            taskfile: serializator.SerializableFile) -> None:
        taskfile.persistence_to_localfs(dirpath)
        # 创建 __init__.py
        while True:
            dirpath, _ = os.path.split(dirpath)
            if not dirpath:
                break
            os.system("touch {}".format(
                os.path.join(dirpath, "__init__.py")))

    # --------------------------- init for start service ----------------------------
    def init_for_start_service(self):
        if not self._is_source_op():
            self.input_channel = self._init_input_receiver(
                    self.input_endpoints)
        else:
            # 用来接收 checkpoint 等 event
            self.input_channel = multiprocessing.Queue()
        if not self._is_sink_op():
            self.output_channel = self._init_output_dispenser(
                    self.output_endpoints)
        self._start_compute_on_standleton_process(
                self.input_channel, self.output_channel, 
                is_process=stream_lite.config.IS_PROCESS)

    def _is_source_op(self) -> bool:
        cls = SubTaskServicer._import_cls_from_file(
                self.cls_name, os.path.join(
                    self.taskfile_dir, self.task_filename))
        return issubclass(cls, operator.SourceOperatorBase)

    def _is_sink_op(self) -> bool:
        cls = SubTaskServicer._import_cls_from_file(
                self.cls_name, os.path.join(
                    self.taskfile_dir, self.task_filename))
        return issubclass(cls, operator.SinkOperatorBase)

    def _init_input_receiver(self, input_endpoints: List[str]) \
            -> multiprocessing.Queue:
        input_channel = multiprocessing.Queue()
        self.input_receiver = InputReceiver(
                input_channel, input_endpoints)
        return input_channel

    def _init_output_dispenser(self, output_endpoints: List[str]) \
            -> multiprocessing.Queue:
        output_channel = multiprocessing.Queue()
        self.output_dispenser = OutputDispenser(
                output_channel, output_endpoints,
                self.subtask_name, self.partition_idx,
                self.downstream_cls_names)
        return output_channel

    def _start_compute_on_standleton_process(self,
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue,
            is_process=True):
        if self._core_process is not None:
            raise SystemExit("Failed: process already running")
        if is_process:
            self._core_process = multiprocessing.Process(
                    target=SubTaskServicer._compute_core, 
                    args=(
                        self.jobid,
                        os.path.join(
                            self.taskfile_dir, self.task_filename), 
                        self.cls_name, self.subtask_name, 
                        self.resource_path_dict, 
                        input_channel, output_channel,
                        self.upstream_cls_names,
                        self.downstream_cls_names,
                        self.snapshot_dir,
                        self.job_manager_enpoint,
                        self.state),
                    daemon=True)
        else:
            self._core_process = threading.Thread(
                    target=SubTaskServicer._compute_core,
                    args=(
                        self.jobid, 
                        os.path.join(
                            self.taskfile_dir, self.task_filename),
                        self.cls_name, self.subtask_name,
                        self.resource_path_dict,
                        input_channel, output_channel,
                        self.upstream_cls_names,
                        self.downstream_cls_names,
                        self.snapshot_dir,
                        self.job_manager_enpoint,
                        self.state))
        self._core_process.start()

    # --------------------------- compute core ----------------------------
    @staticmethod
    def _compute_core( 
            jobid: str,
            full_task_filename: str,
            cls_name: str,
            subtask_name: str,
            resource_path_dict: Dict[str, str],
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue,
            upstream_cls_names: List[str],
            downstream_cls_names: List[str],
            snapshot_dir: str,
            job_manager_enpoint: str,
            state: Union[None, task_manager_pb2.DeployTaskRequest.State]):
        try:
            SubTaskServicer._inner_compute_core(
                    jobid, full_task_filename, cls_name, subtask_name,
                    resource_path_dict, input_channel, output_channel,
                    upstream_cls_names, downstream_cls_names,
                    snapshot_dir, job_manager_enpoint, state)
        except FinishJobError as e:
            # SourceOp 中通过 FinishJobError 异常来表示
            # 处理完成。向下游 Operator 发送 Finish Record
            cls = SubTaskServicer._import_cls_from_file(
                    cls_name, full_task_filename)
            if issubclass(cls, operator.SourceOperatorBase):
                _LOGGER.info(
                        "[{}] finished successfully!".format(subtask_name))
                SubTaskServicer._push_finish_event_to_output_channel(output_channel)
            else:
                _LOGGER.critical(
                        "Failed: run {} task failed (reason: {})".format(
                            subtask_name, e), exc_info=True)
                os._exit(-1)
        except Exception as e:
            _LOGGER.critical(
                    "Failed: run {} task failed (reason: {})".format(
                        subtask_name, e), exc_info=True)
            os._exit(-1)

    @staticmethod
    def _inner_compute_core( 
            jobid: str,
            full_task_filename: str,
            cls_name: str,
            subtask_name: str,
            resource_path_dict: Dict[str, str],
            input_channel: multiprocessing.Queue,
            output_channel: multiprocessing.Queue,
            upstream_cls_names: List[str],
            downstream_cls_names: List[str],
            snapshot_dir: str,
            job_manager_enpoint: str,
            state_pb: Union[None, task_manager_pb2.DeployTaskRequest.State]):
        """
        具体执行逻辑
        """
        cls = SubTaskServicer._import_cls_from_file(
                cls_name, full_task_filename)
        if not issubclass(cls, operator.OperatorBase):
            raise TypeError(
                    "Failed: {} is not a subclass of OperatorBase".format(cls_name))
        is_source_op = issubclass(cls, operator.SourceOperatorBase)
        is_sink_op = issubclass(cls, operator.SinkOperatorBase)
        is_key_op = issubclass(cls, operator.KeyOperatorBase)
        migrate_window = MigrateFilterWindow() # 为了过滤 migrate 产生的重复数据
        migrate_id = -1 # 标记是否正处于 migrate
        task_instance = cls()
        task_instance.set_name(subtask_name)
        task_instance.init(resource_path_dict)
        #  _LOGGER.info("snapshot_dir: {}".format(snapshot_dir))

        # restore from checkpoint (load state)
        if state_pb is not None:
            if state_pb.type == "":
                pass
            elif state_pb.type == "FILE":
                seri_state = serializator.SerializableFile.from_proto(state_pb.state_file)
                if seri_state.content:
                    state = pickle.loads(seri_state.content)
                    task_instance.restore_from_checkpoint(state)
            elif state_pb.type == "LOCAL":
                file_path = os.path.join(
                        snapshot_dir, "chk_{}".format(state_pb.local_checkpoint_id))
                with open(file_path, "rb") as f:
                    state = pickle.loads(f.read())
                    task_instance.restore_from_checkpoint(state)
            else:
                raise TypeError("Failed load state: unknow state type ({})".format(state_pb.type)) 

        # --------------------- get input ----------------------
        def get_input_data(
                task_instance: operator.OperatorBase,
                is_source_op: bool) \
                        -> Tuple[int, Any, str, int]:
            if not is_source_op:
                record = input_channel.get() # SerializableRecord
                return [
                        record.data_type, # type
                        record.data.data, # input_data (SerializableData.data)
                        record.data_id,   # data_id
                        record.timestamp] # timestamp
            else:
                try:
                    record = input_channel.get_nowait()
                    return [
                            record.data_type, # type
                            record.data.data, # input_data
                            -1,               # data_id
                            -1]               # timestamp
                except queue.Empty as e:
                    # SourceOp 没有 event，继续处理
                    data_id = str(DataIdGenerator().next())
                    data_type = common_pb2.Record.DataType.PICKLE
                    timestamp = util.get_timestamp()
                    return [
                            data_type, # data_type
                            None,      # input_data
                            data_id,   # data_id
                            timestamp] # timestamp 

        # --------------------- process data ----------------------
        def pickle_data_process(
                task_instance: operator.OperatorBase,
                is_key_op: bool,
                input_data: Any) -> Tuple[Any, int]:
            output_data = None
            partition_key = -1
            if is_key_op:
                output_data = input_data
                partition_key = task_instance.compute(input_data)
            else:
                output_data = task_instance.compute(input_data)
            return output_data, partition_key

        def checkpoint_event_process(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                input_data: common_pb2.Record.Checkpoint) -> None:
            if not is_sink_op:
                SubTaskServicer._push_checkpoint_event_to_output_channel(
                        input_data, output_channel)
            SubTaskServicer._checkpoint(
                    task_instance=task_instance,
                    snapshot_dir=snapshot_dir,
                    checkpoint=input_data,
                    checkpoint_id=input_data.id,
                    job_manager_enpoint=job_manager_enpoint,
                    subtask_name=subtask_name,
                    jobid=jobid)

        def checkpoint_prepare_for_migrate_event_process(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                input_data: common_pb2.Record.Checkpoint) -> None:
            if not is_sink_op:
                SubTaskServicer._push_checkpoint_prepare_for_migrate_event_to_output_channel(
                        input_data, output_channel)
            SubTaskServicer._checkpoint(
                    task_instance=task_instance,
                    snapshot_dir=snapshot_dir,
                    checkpoint=input_data,
                    checkpoint_id=input_data.id,
                    job_manager_enpoint=job_manager_enpoint,
                    subtask_name=subtask_name,
                    jobid=jobid)

        def migrate_event_process(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                input_data: common_pb2.Record.MIGRATE) -> None:
            if not is_sink_op:
                SubTaskServicer._push_migrate_event_to_output_channel(
                        input_data, output_channel)
            SubTaskServicer._migrate(
                    task_instance=task_instance,
                    migrate=input_data,
                    job_manager_enpoint=job_manager_enpoint,
                    subtask_name=subtask_name,
                    upstream_cls_names=upstream_cls_names,
                    downstream_cls_names=downstream_cls_names,
                    migrate_id=input_data.id,
                    jobid=jobid)

        def terminate_event_process(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                input_data: common_pb2.Record.TERMINATE_SUBTASK) -> None:
            if not is_sink_op:
                SubTaskServicer._push_terminate_event_to_output_channel(
                        input_data, output_channel)
            SubTaskServicer._terminate_subtask(
                    task_instance=task_instance,
                    terminate=input_data,
                    job_manager_enpoint=job_manager_enpoint,
                    subtask_name=subtask_name,
                    terminate_id=input_data.id,
                    jobid=jobid)

        def finish_event_process(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                input_data: Any) -> None:
            if is_sink_op:
                return
            SubTaskServicer._push_finish_event_to_output_channel(output_channel)

        # --------------------- push output ----------------------
        def push_output_data(
                task_instance: operator.OperatorBase,
                is_sink_op: bool,
                output_data: Any,
                data_type: int,
                data_id: str,
                timestamp: int,
                partition_key: int) -> None:
            if is_sink_op:
                now_timestamp = util.get_timestamp()
                _LOGGER.info("P[{}] latency: {}ms".format(
                    data_id, now_timestamp - timestamp))
                return
            output = serializator.SerializableData.from_object(
                    output_data, data_type=data_type)
            output_channel.put(
                    serializator.SerializableRecord(
                        data_id=data_id,
                        data_type=output.data_type,
                        data=output,
                        timestamp=timestamp,
                        partition_key=partition_key))

        #  print("[{}] succ run".format(subtask_name))
        while True:
            partition_key = -1
            output_data = None

            data_type, input_data, data_id, timestamp = \
                    get_input_data(task_instance, is_source_op)
            
            if data_type == common_pb2.Record.DataType.PICKLE:
                is_duplicate, stream_sync = \
                        migrate_window.duplicate_or_update(int(data_id))
                if is_duplicate:
                    if migrate_id != -1 and stream_sync:
                        # 过滤重复 data_id: 新旧数据流已经同步，可以终止旧数据流
                        _LOGGER.info(
                                "[{}] stream sync({})! try to terminate the old subtask."
                                .format(subtask_name, data_id))
                    
                        SubTaskServicer._notify_migrate_synchron(
                                job_manager_enpoint=job_manager_enpoint,
                                jobid=jobid,
                                migrate_id=migrate_id)
                        migrate_id = -1
                    continue
                output_data, partition_key = pickle_data_process(
                        task_instance=task_instance,
                        is_key_op=is_key_op, 
                        input_data=input_data)
            elif data_type == common_pb2.Record.DataType.CHECKPOINT:
                _LOGGER.info("[{}] recv checkpoint event".format(subtask_name))
                checkpoint_event_process(
                        task_instance=task_instance, 
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.debug("[{}] success save snapshot state".format(subtask_name))
                cancel_job = input_data.cancel_job
                if cancel_job:
                    _LOGGER.info("[{}] success finish job".format(subtask_name))
                    break
                else:
                    continue
            elif data_type == common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE:
                _LOGGER.info("[{}] recv checkpoint_prepare_for_migrate event".format(subtask_name))
                checkpoint_prepare_for_migrate_event_process(
                        task_instance=task_instance, 
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.debug("[{}] success save snapshot state for migrate".format(subtask_name))
                continue
            elif data_type == common_pb2.Record.DataType.MIGRATE:
                _LOGGER.info("[{}] recv migrate event".format(subtask_name))
                assert migrate_id == -1
                migrate_id = input_data.id

                migrate_event_process(
                        task_instance=task_instance,
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                continue
            elif data_type == common_pb2.Record.DataType.FINISH:
                finish_event_process(
                        task_instance=task_instance,
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.info("[{}] finished successfully!".format(subtask_name))
                break
            elif data_type == common_pb2.Record.DataType.TERMINATE_SUBTASK:
                _LOGGER.info("[{}] recv terminate event".format(subtask_name))
                terminate_event_process(
                        task_instance=task_instance,
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.info("[{}] process terminate successfully!".format(subtask_name))
                continue
            else:
                raise Exception("Failed: unknow data type: {}".format(data_type))
            
            push_output_data(
                task_instance=task_instance,
                is_sink_op=is_sink_op,
                output_data=output_data,
                data_type=data_type,
                data_id=data_id,
                timestamp=timestamp,
                partition_key=partition_key)

    @staticmethod
    def _import_cls_from_file(cls_name: str, filepath: str):
        if cls_name in operator.BUILDIN_OPS:
            return operator.BUILDIN_OPS[cls_name]
        if not filepath.endswith(".py"):
            raise SystemExit(
                    "Failed: can not import module '{}'".format(filepath))
        dirpath, filename = os.path.split(filepath)
        module_path = "{}.{}".format(
                dirpath.replace("/", "."), filename.split(".")[0])
        module = importlib.import_module(module_path)
        cls = getattr(module, cls_name)
        return cls

    @staticmethod
    def _push_event_record_to_output_channel(
            data_id: str,
            data_type: common_pb2.Record.DataType,
            data: serializator.SerializableData,
            output_channel: multiprocessing.Queue):
        output_channel.put(
                serializator.SerializableRecord(
                    data_id=data_id,
                    data_type=data_type,
                    data=data,
                    timestamp=util.get_timestamp(),
                    partition_key=-1))

    @staticmethod
    def _push_finish_event_to_output_channel(
            output_channel: multiprocessing.Queue):
        SubTaskServicer._push_event_record_to_output_channel(
                data_id="last_finish_data_id",
                data_type=common_pb2.Record.DataType.FINISH,
                data=serializator.SerializableData.from_object(
                    data=common_pb2.Record.Finish(),
                    data_type=common_pb2.Record.DataType.FINISH),
                output_channel=output_channel)

    @staticmethod
    def _push_checkpoint_event_to_output_channel(
            checkpoint: common_pb2.Record.Checkpoint,
            output_channel: multiprocessing.Queue):
        SubTaskServicer._push_event_record_to_output_channel(
                data_id="checkpoint_data_id",
                data_type=common_pb2.Record.DataType.CHECKPOINT,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.CHECKPOINT,
                    data=checkpoint),
                output_channel=output_channel)
    
    @staticmethod
    def _push_checkpoint_prepare_for_migrate_event_to_output_channel(
            checkpoint: common_pb2.Record.CheckpointPrepareForMigrate,
            output_channel: multiprocessing.Queue) -> None:
        SubTaskServicer._push_event_record_to_output_channel(
                data_id="checkpoint_prepare_for_migrate_data_id",
                data_type=common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE,
                    data=checkpoint),
                output_channel=output_channel)

    @staticmethod
    def _push_migrate_event_to_output_channel(
            migrate: common_pb2.Record.MIGRATE,
            output_channel: multiprocessing.Queue):
        SubTaskServicer._push_event_record_to_output_channel(
                data_id="migrate_data_id",
                data_type=common_pb2.Record.DataType.MIGRATE,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.MIGRATE,
                    data=migrate),
                output_channel=output_channel)
    
    @staticmethod
    def _push_terminate_event_to_output_channel(
            terminate: common_pb2.Record.TERMINATE_SUBTASK,
            output_channel: multiprocessing.Queue):
        SubTaskServicer._push_event_record_to_output_channel(
                data_id="terminate_data_id",
                data_type=common_pb2.Record.DataType.TERMINATE_SUBTASK,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.TERMINATE_SUBTASK,
                    data=terminate),
                output_channel=output_channel)

    @staticmethod
    def _save_snapshot_state(
            snapshot_dir: str,
            snapshot_state: Any,
            checkpoint_id: int) -> serializator.SerializableFile:
        seri_file = serializator.SerializableFile(
                name="chk_{}".format(checkpoint_id),
                content=pickle.dumps(snapshot_state))
        seri_file.persistence_to_localfs(prefix_path=snapshot_dir)
        return seri_file

    @staticmethod
    def _acknowledge_checkpoint(
            job_manager_enpoint: str,
            subtask_name: str, 
            jobid: str,
            checkpoint_id: int, 
            state: serializator.SerializableFile,
            err_code: int = 0, 
            err_msg: str = "") -> None:
        client = JobManagerClient()
        client.connect(job_manager_enpoint)
        client.acknowledgeCheckpoint(
                subtask_name=subtask_name,
                jobid=jobid,
                checkpoint_id=checkpoint_id,
                state=state,
                err_code=err_code,
                err_msg=err_msg)

    @staticmethod
    def _acknowledge_migrate(
            job_manager_enpoint: str,
            subtask_name: str, 
            jobid: str,
            migrate_id: int, 
            err_code: int = 0, 
            err_msg: str = "") -> None:
        client = JobManagerClient()
        client.connect(job_manager_enpoint)
        client.acknowledgeMigrate(
                subtask_name=subtask_name,
                jobid=jobid,
                migrate_id=migrate_id,
                err_code=err_code,
                err_msg=err_msg)
    
    @staticmethod
    def _acknowledge_terminate(
            job_manager_enpoint: str,
            subtask_name: str, 
            jobid: str,
            terminate_id: int, 
            err_code: int = 0, 
            err_msg: str = "") -> None:
        client = JobManagerClient()
        client.connect(job_manager_enpoint)
        client.acknowledgeTerminate(
                subtask_name=subtask_name,
                jobid=jobid,
                terminate_id=terminate_id,
                err_code=err_code,
                err_msg=err_msg)
    
    @staticmethod
    def _notify_migrate_synchron(
            job_manager_enpoint: str,
            jobid: str,
            migrate_id: int) -> None:
        client = JobManagerClient()
        client.connect(job_manager_enpoint)
        client.notifyMigrateSynchron(
                jobid=jobid, migrate_id=migrate_id)

    @staticmethod
    def _checkpoint(
            task_instance: operator.OperatorBase,
            snapshot_dir: str,
            checkpoint: Any,
            checkpoint_id: int,
            job_manager_enpoint: str,
            subtask_name: str,
            jobid: str) -> None:
        """
        每个 subtask 执行 checkpoint 操作
        """
        snapshot_state = task_instance.checkpoint()
        seri_file = SubTaskServicer._save_snapshot_state(
                snapshot_dir, snapshot_state, checkpoint_id)
        SubTaskServicer._acknowledge_checkpoint(
                job_manager_enpoint=job_manager_enpoint, 
                subtask_name=subtask_name, 
                jobid=jobid,
                checkpoint_id=checkpoint_id,
                state=seri_file)

    @staticmethod
    def _migrate(
            task_instance: operator.OperatorBase,
            migrate_id: int,
            migrate: common_pb2.Record.Migrate,
            job_manager_enpoint: str,
            subtask_name: str,
            upstream_cls_names: List[str],
            downstream_cls_names: List[str],
            jobid: str) -> None:
        """
        每个 subtask 执行 migrate 操作
        """
        return
        #TODO: skipping
        # (这部分操作在 output_dispenser 处理): 
        #   1. new_subtask_name 上游的 task 与之建立连接
        SubTaskServicer._acknowledge_migrate(
                job_manager_enpoint=job_manager_enpoint,
                subtask_name=subtask_name,
                jobid=jobid,
                migrate_id=migrate_id)

    @staticmethod
    def _terminate_subtask(
            task_instance: operator.OperatorBase,
            terminate_id: int,
            terminate: common_pb2.Record.TerminateSubtask,
            job_manager_enpoint: str,
            subtask_name: str,
            jobid: str) -> None:
        """
        每个 subtask 执行 migrate 操作
        """
        # (这部分操作在 output_dispenser 处理): 
        #   1. 上游 task 断开对旧 subtask_name 的连接
        #   2. 终止旧 subtask_name
        SubTaskServicer._acknowledge_terminate(
                job_manager_enpoint=job_manager_enpoint,
                subtask_name=subtask_name,
                jobid=jobid,
                terminate_id=terminate_id)

    # --------------------------- pushRecord (recv) ----------------------------
    def pushRecord(self, request, context):
        pre_subtask = request.from_subtask
        partition_idx = request.partition_idx
        record = request.record

        # migrate 过程中，过滤掉所有需要 barrier 的 event
        if pre_subtask.endswith("@MIGRATE"):
            if record.data_type != common_pb2.Record.DataType.PICKLE:
                return gen_nil_response()

        #  _LOGGER.info("Recv data(from={}): {}".format(
            #  pre_subtask, str(partition_idx)))
        # FIXME: 有时候partition_idx会异常
        self.input_receiver.recv_data(partition_idx, record)
        return gen_nil_response()

    # --------------------------- triggerCheckpoint ----------------------------
    def triggerCheckpoint(self, request, context):
        """
        只有 SourceOp 才会被调用该函数
        """
        checkpoint = request.checkpoint
        seri_data = serializator.SerializableRecord(
                data_id="checkpoint_data_id",
                data_type=common_pb2.Record.DataType.CHECKPOINT,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.CHECKPOINT,
                    data=checkpoint),
                timestamp=util.get_timestamp(),
                partition_key=-1)
        self.input_channel.put(seri_data)
        return gen_nil_response()

    # --------------------------- triggerCheckpointPrepareForMigrate ----------------------------
    def triggerCheckpointPrepareForMigrate(self, request, context):
        """
        只有 SourceOp 才会被调用该函数
        """
        checkpoint = request.checkpoint
        seri_data = serializator.SerializableRecord(
                data_id="checkpoint_data_id",
                data_type=common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE,
                    data=checkpoint),
                timestamp=util.get_timestamp(),
                partition_key=-1)
        self.input_channel.put(seri_data)
        return gen_nil_response()

    # --------------------------- triggerCheckpoint ----------------------------
    def triggerMigrate(self, request, context):
        """
        只有 SourceOp 才会被调用该函数
        """
        migrate = request.migrate
        seri_data = serializator.SerializableRecord(
                data_id="migrate_data_id",
                data_type=common_pb2.Record.DataType.MIGRATE,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.MIGRATE,
                    data=migrate),
                timestamp=util.get_timestamp(),
                partition_key=-1)
        self.input_channel.put(seri_data)
        return gen_nil_response()

    # --------------------------- terminateSubtask ----------------------------
    def terminateSubtask(self, request, context):
        """
        只有 SourceOp 才会被调用该函数
        """
        terminate = request.terminate_subtask
        seri_data = serializator.SerializableRecord(
                data_id="terminate_data_id",
                data_type=common_pb2.Record.DataType.TERMINATE_SUBTASK,
                data=serializator.SerializableData.from_object(
                    data_type=common_pb2.Record.DataType.TERMINATE_SUBTASK,
                    data=terminate),
                timestamp=util.get_timestamp(),
                partition_key=-1)
        self.input_channel.put(seri_data)
        return gen_nil_response()
