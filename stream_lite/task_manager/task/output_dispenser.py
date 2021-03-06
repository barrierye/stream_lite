#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import os
import logging
import threading
import multiprocessing
import time
import grpc
import queue
from typing import List, Dict, Union

from stream_lite.proto import common_pb2

import stream_lite.config
from stream_lite.client import SubTaskClient
from stream_lite.network import serializator

from . import partitioner

_LOGGER = logging.getLogger(__name__)


class OutputDispenser(object):
    """
                                                 / PartitionDispenser
    SubTask   ->   channel   ->    Dispenser     - PartitionDispenser   ->  SubTask
                            (standalone process) \ PartitionDispenser
                                                     (SubTaskClient)
    """

    def __init__(self, 
            output_channel: multiprocessing.Queue,
            output_endpoints: List[str],
            subtask_name: str,
            partition_idx: int,
            downstream_cls_names: List[str]):
        """
        这里 output_endpoints 是按 partition_idx 顺序排列的
        """
        self.channel = output_channel
        self.output_endpoints = output_endpoints
        self.subtask_name = subtask_name
        self.partition_idx = partition_idx
        self.downstream_cls_names = downstream_cls_names
        assert len(self.downstream_cls_names) <= 1
        self._process = self.start_standleton_process(
                is_process=stream_lite.config.IS_PROCESS)

    def push_data(self, data: serializator.SerializableRecord) -> None:
        self.channel.put(data)

    def _partitioning_data_and_carry_to_next_subtask(self,
            input_channel: multiprocessing.Queue,
            output_endpoints: List[str],
            subtask_name: str,
            partition_idx: int,
            downstream_cls_names: List[str]):
        try:
            self._inner_partitioning_data_and_carry_to_next_subtask(
                    input_channel, output_endpoints, subtask_name, 
                    partition_idx, downstream_cls_names)
        except Exception as e:
            _LOGGER.critical(
                    "Failed: [{}] run output_partition_dispenser failed ({})"
                    .format(self.subtask_name, e), exc_info=True)
            os._exit(-1)

    def _inner_partitioning_data_and_carry_to_next_subtask(self,
            input_channel: multiprocessing.Queue,
            output_endpoints: List[str],
            subtask_name: str,
            partition_idx: int,
            downstream_cls_names: List[str]):
        partitions = {} # partition_idx -> output_partition_dispenser
        for idx, endpoint in enumerate(output_endpoints):
            output_partition_dispenser = OutputPartitionDispenser(
                    endpoint, subtask_name, partition_idx)
            output_partition_dispenser.run_on_standalone_thread()
            partitions[idx] = [output_partition_dispenser]

        partition_num = len(partitions)

        need_broadcast_datatype = [
                common_pb2.Record.DataType.FINISH,
                common_pb2.Record.DataType.CHECKPOINT,
                common_pb2.Record.DataType.MIGRATE,
                common_pb2.Record.DataType.TERMINATE_SUBTASK,
                common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE]

        while True:
            seri_record = input_channel.get()
            #  print("name: {}, type: {}, data: {}".format(subtask_name, seri_record.data_type, seri_record.data.data))
            if seri_record.data_type in need_broadcast_datatype:
                # broadcast
                for output_partition_dispensers in partitions.values():
                    for dispenser in output_partition_dispensers:
                        dispenser.push_data(seri_record)
                
                if seri_record.data_type == common_pb2.Record.DataType.CHECKPOINT_PREPARE_FOR_MIGRATE:
                    # checkpoint_prepare_for_migrate: 为下游 task 创建新的 dispenser
                    checkpoint_prepare_for_migrate = seri_record.data.data
                    migrate_cls_name = checkpoint_prepare_for_migrate.migrate_cls_name
                    migrate_partition_idx = checkpoint_prepare_for_migrate.migrate_partition_idx
                    if migrate_cls_name == downstream_cls_names[0]:
                        partitions[migrate_partition_idx].append(
                                OutputPartitionDispenser(
                                    endpoint=None,
                                    subtask_name=subtask_name,
                                    partition_idx=partition_idx))
                elif seri_record.data_type == common_pb2.Record.DataType.CHECKPOINT:
                    # 与checkpoint_prepare_for_migrate类似，为下游 task 创建新的 dispenser
                    # 与之不同的是，新的checkpoint会替换掉旧的dispenser
                    checkpoint_prepare_for_migrate = seri_record.data.data
                    migrate_cls_name = checkpoint_prepare_for_migrate.migrate_cls_name
                    migrate_partition_idx = checkpoint_prepare_for_migrate.migrate_partition_idx
                    if migrate_cls_name == downstream_cls_names[0]:
                        assert len(partitions[migrate_partition_idx]) <= 2
                        if len(partitions[migrate_partition_idx]) == 2:
                            partitions[migrate_partition_idx].pop(-1)
                        partitions[migrate_partition_idx].append(
                                OutputPartitionDispenser(
                                    endpoint=None,
                                    subtask_name=subtask_name,
                                    partition_idx=partition_idx))
                elif seri_record.data_type == common_pb2.Record.DataType.MIGRATE:
                    # migrate event: 启动之前创建的 dispenser 
                    migrate = seri_record.data.data
                    new_cls_name = migrate.new_cls_name
                    new_partition_idx = migrate.new_partition_idx
                    new_endpoint = migrate.new_endpoint
                    #TODO: IndexError: list index out of range
                    if new_cls_name == downstream_cls_names[0]:
                        partitions[new_partition_idx][1].connect(new_endpoint)
                        partitions[new_partition_idx][1].run_on_standalone_thread()
                elif seri_record.data_type == common_pb2.Record.DataType.TERMINATE_SUBTASK:
                    # terminate event: 上游关闭与旧 subtask 的连接，旧 subtask 停止
                    terminate = seri_record.data.data
                    terminate_cls_name = terminate.cls_name
                    terminate_partition_idx = terminate.partition_idx
                    terminate_subtask_name = terminate.subtask_name
                    _LOGGER.info(
                            "[{}] get terminate ({})...".format(
                                self.subtask_name, terminate_cls_name))
                    if terminate_cls_name == downstream_cls_names[0]:
                        _LOGGER.info("[{}] going to close downstream({}) connect...".format(
                            self.subtask_name, terminate_cls_name))
                        assert len(partitions[terminate_partition_idx]) == 2
                        dispenser = partitions[migrate_partition_idx].pop(0)
                        dispenser.close()
            else:
                # partitioning
                partition_idx = -1
                if seri_record.partition_key != -1:
                    partition_idx = partitioner.KeyPartitioner.partitioning(
                            seri_record, partition_num)
                else:
                    partition_idx = partitioner.RandomPartitioner.partitioning(
                            seri_record, partition_num)
                for dispenser in partitions[partition_idx]:
                    dispenser.push_data(seri_record)

    def start_standleton_process(self, is_process):
        if is_process:
            proc = multiprocessing.Process(
                    target=self._partitioning_data_and_carry_to_next_subtask,
                    args=(self.channel, self.output_endpoints,
                        self.subtask_name, self.partition_idx,
                        self.downstream_cls_names),
                    daemon=True)
        else:
            proc = threading.Thread(
                    target=self._partitioning_data_and_carry_to_next_subtask,
                    args=(self.channel, self.output_endpoints,
                        self.subtask_name, self.partition_idx,
                        self.downstream_cls_names))
        proc.start()
        return proc


class OutputPartitionDispenser(object):

    def __init__(self, 
            endpoint: Union[None, str], 
            subtask_name: str, 
            partition_idx: int):
        """
        如果 endpoint 为 None，则为正常的 OutputPartitionDispenser；
        反之则为 migrate 专用的 OutputPartitionDispenser，不初始化 endpoint
        """
        self.subtask_name = subtask_name
        self.partition_idx = partition_idx
        self.client = SubTaskClient()

        self.is_connect_completed = False # 完成下游节点连接
        self.data_buffer = queue.Queue()
        self.connect(endpoint)

        self._standalone_thread = None

    def connect(self, endpoint: Union[None, str]) -> bool:
        if endpoint:
            _LOGGER.debug(
                    "[{}] Try to connect to endpoint: {}".format(
                        self.subtask_name, endpoint))
            self.client.connect(endpoint)
            self.is_connect_completed = True
            
    def push_data(self, record: serializator.SerializableRecord) -> None:
        # 这里可能会把 event 放入 buffer，但没有啥影响
        self.data_buffer.put(record)

    def run_on_standalone_thread(self) -> None:
        if self._standalone_thread:
            raise RuntimeError("")
        self._standalone_thread = threading.Thread(
                target=self._inner_push_data,
                args=(self.data_buffer, ))
        self._standalone_thread.start()

    def _inner_push_data(self, data_buffer: queue.Queue) -> None:
        while True:
            data = data_buffer.get()
            while True:
                try:
                    self.client.pushRecord(
                                from_subtask=self.subtask_name,
                                partition_idx=self.partition_idx,
                                record=data.instance_to_proto())
                except grpc._channel._InactiveRpcError as e:
                    _LOGGER.warning(
                            "Failed to push data: Maybe downstream not prepared"
                            " yet, wait for 10ms...")
                    time.sleep(0.01)
                    continue
                break

    def close(self) -> None:
        #TODO: close OutputPartitionDispenser, 线程阻塞
        pass
