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

from stream_lite.network import serializator
from stream_lite.network.util import gen_nil_response
from stream_lite.client import SubTaskClient, JobManagerClient
from stream_lite.utils import util, FinishJobError, DataIdGenerator

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
            state: Union[None, common_pb2.File]):
        super(SubTaskServicer, self).__init__()
        self.tm_name = tm_name
        self.jobid = jobid
        self.job_manager_enpoint = job_manager_enpoint
        self.subtask_id = id(self)
        self.cls_name = execute_task.cls_name
        self.task_filename = execute_task.task_file.name
        self.input_endpoints = execute_task.input_endpoints
        self.output_endpoints = execute_task.output_endpoints
        self.subtask_name = execute_task.subtask_name
        self.partition_idx = execute_task.partition_idx
        self.port = execute_task.port
        self.state = state
        self.resource_dir = "_tmp/tm/{}/{}/partition_{}/resource".format(
                self.tm_name, self.cls_name, self.partition_idx)
        self.taskfile_dir = "_tmp/tm/{}/{}/partition_{}/taskfile".format(
                self.tm_name, self.cls_name, self.partition_idx)
        self.snapshot_dir = "_tmp/tm/{}/{}/partition_{}/snapshot".format(
                self.tm_name, self.cls_name, self.partition_idx)
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
                self.subtask_name, self.partition_idx)
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
            snapshot_dir: str,
            job_manager_enpoint: str,
            state: Union[None, common_pb2.File]):
        try:
            SubTaskServicer._inner_compute_core(
                    jobid, full_task_filename, cls_name, subtask_name,
                    resource_path_dict, input_channel, output_channel,
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
            snapshot_dir: str,
            job_manager_enpoint: str,
            state_pb: Union[None, common_pb2.File]):
        """
        具体执行逻辑
        """
        cls = SubTaskServicer._import_cls_from_file(
                cls_name, full_task_filename)
        if not issubclass(cls, operator.OperatorBase):
            raise SystemExit(
                    "Failed: {} is not a subclass of OperatorBase".format(cls_name))
        is_source_op = issubclass(cls, operator.SourceOperatorBase)
        is_sink_op = issubclass(cls, operator.SinkOperatorBase)
        is_key_op = issubclass(cls, operator.KeyOperatorBase)
        task_instance = cls()
        task_instance.set_name(subtask_name)
        task_instance.init(resource_path_dict)

        # restore from checkpoint
        if state_pb is not None:
            seri_state = serializator.SerializableFile.from_proto(state_pb)
            if seri_state.content:
                state = pickle.loads(seri_state.content)
                task_instance.restore_from_checkpoint(state)

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
                input_data: Any) -> None:
            if not is_sink_op:
                SubTaskServicer._push_checkpoint_event_to_output_channel(
                        input_data, output_channel)
            SubTaskServicer._checkpoint(
                    task_instance=task_instance,
                    snapshot_dir=snapshot_dir,
                    checkpoint_id=input_data.id,
                    job_manager_enpoint=job_manager_enpoint,
                    subtask_name=subtask_name,
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


        while True:
            partition_key = -1
            output_data = None

            data_type, input_data, data_id, timestamp = \
                    get_input_data(task_instance, is_source_op)
            
            if data_type == common_pb2.Record.DataType.PICKLE:
                output_data, partition_key = pickle_data_process(
                        task_instance=task_instance,
                        is_key_op=is_key_op, 
                        input_data=input_data)
            elif data_type == common_pb2.Record.DataType.CHECKPOINT:
                checkpoint_event_process(
                        task_instance=task_instance, 
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.info("[{}] success save snapshot state".format(subtask_name))
                cancel_job = input_data.cancel_job
                if cancel_job:
                    _LOGGER.info("[{}] success finish job".format(subtask_name))
                    break
                else:
                    continue
            elif data_type == common_pb2.Record.DataType.FINISH:
                finish_event_process(
                        task_instance=task_instance,
                        is_sink_op=is_sink_op,
                        input_data=input_data)
                _LOGGER.info("[{}] finished successfully!".format(subtask_name))
                break
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
    def _checkpoint(
            task_instance: operator.OperatorBase,
            snapshot_dir: str,
            checkpoint_id: int,
            job_manager_enpoint: str,
            subtask_name: str,
            jobid: str) -> None:
        snapshot_state = task_instance.checkpoint()
        seri_file = SubTaskServicer._save_snapshot_state(
                snapshot_dir, snapshot_state, checkpoint_id)
        SubTaskServicer._acknowledge_checkpoint(
                job_manager_enpoint=job_manager_enpoint, 
                subtask_name=subtask_name, 
                jobid=jobid,
                checkpoint_id=checkpoint_id,
                state=seri_file)

    # --------------------------- pushRecord (recv) ----------------------------
    def pushRecord(self, request, context):
        pre_subtask = request.from_subtask
        partition_idx = request.partition_idx
        record = request.record
        _LOGGER.debug("Recv data(from={}): {}".format(
            pre_subtask, str(request)))
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
