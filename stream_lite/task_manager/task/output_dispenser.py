#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import os
import logging
import threading
import multiprocessing
from typing import List, Dict

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
            partition_idx: int):
        """
        这里 output_endpoints 是按 partition_idx 顺序排列的
        """
        self.channel = output_channel
        self.output_endpoints = output_endpoints
        self.subtask_name = subtask_name
        self.partition_idx = partition_idx
        self._process = self.start_standleton_process(
                is_process=stream_lite.config.IS_PROCESS)

    def push_data(self, data: serializator.SerializableRecord) -> None:
        self.channel.put(data)

    def _partitioning_data_and_carry_to_next_subtask(self,
            input_channel: multiprocessing.Queue,
            output_endpoints: List[str],
            subtask_name: str,
            partition_idx: int,
            succ_start_service_event: multiprocessing.Event):
        try:
            self._inner_partitioning_data_and_carry_to_next_subtask(
                    input_channel, output_endpoints, subtask_name, 
                    partition_idx, succ_start_service_event)
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
            succ_start_service_event: multiprocessing.Event):
        partitions = []
        for endpoint in output_endpoints:
            output_partition_dispenser = OutputPartitionDispenser(
                    endpoint, subtask_name, partition_idx)
            partitions.append(output_partition_dispenser)

        partition_num = len(partitions)

        need_broadcast_datatype = [
                common_pb2.Record.DataType.FINISH,
                common_pb2.Record.DataType.CHECKPOINT]
        succ_start_service_event.set()
        while True:
            seri_record = input_channel.get()
            if seri_record.data_type in need_broadcast_datatype:
                # broadcast
                for output_partition_dispenser in partitions:
                    output_partition_dispenser.push_data(seri_record)
            else:
                # partitioning
                partition_idx = -1
                if seri_record.partition_key != -1:
                    partition_idx = partitioner.KeyPartitioner.partitioning(
                            seri_record, partition_num)
                else:
                    partition_idx = partitioner.RandomPartitioner.partitioning(
                            seri_record, partition_num)
                partitions[partition_idx].push_data(seri_record)

    def start_standleton_process(self, is_process):
        succ_start_service_event = multiprocessing.Event()
        if is_process:
            proc = multiprocessing.Process(
                    target=self._partitioning_data_and_carry_to_next_subtask,
                    args=(self.channel, self.output_endpoints,
                        self.subtask_name, self.partition_idx,
                        succ_start_service_event),
                    daemon=True)
        else:
            proc = threading.Thread(
                    target=self._partitioning_data_and_carry_to_next_subtask,
                    args=(self.channel, self.output_endpoints,
                        self.subtask_name, self.partition_idx,
                        succ_start_service_event))
        proc.start()
        succ_start_service_event.wait()
        return proc


class OutputPartitionDispenser(object):

    def __init__(self, endpoint: str, subtask_name: str, partition_idx: int):
        self.subtask_name = subtask_name
        self.partition_idx = partition_idx
        self.client = SubTaskClient()
        _LOGGER.debug("[{}] Try to connect to endpoint: {}".format(subtask_name, endpoint))
        self.client.connect(endpoint)

    def push_data(self, record: serializator.SerializableRecord) -> None:
        self.client.pushRecord(
                    from_subtask=self.subtask_name,
                    partition_idx=self.partition_idx,
                    record=record.instance_to_proto())
