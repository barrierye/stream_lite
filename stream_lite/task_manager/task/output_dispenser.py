#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import logging
from typing import List, Dict

from stream_lite.client import SubTaskClient

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
        self._process = self.start_standleton_process()

    def push_data(self, data: serializator.SerializableStreamData) -> None:
        self.channel.put(data)

    def _partitioning_data_and_carry_to_next_subtask(self,
            input_channel: multiprocessing.Queue,
            output_endpoints: List[str],
            subtask_name: str,
            partition_idx: int):
        partitions = []
        for endpoint in output_endpoints:
            output_partition_dispenser = OutputPartitionDispenser(
                    endpoint, subtask_name, partition_idx)
            partitions.append(output_partition_dispenser)

        partition_num = len(partitions)
        while True:
            data = input_channel.get()
            if data.data_type == common_pb2.StreamData.DataType.NORMAL:
                # partitioning
                partition_idx = -1
                if data.partition_key:
                    partition_idx = partitioner.KeyPartitioner.partitioning(
                            data, partition_num)
                else:
                    partition_idx = partitioner.RandomPartitioner.partitioning(
                            data, partition_num)
                partitions[partition_idx].push_data(data)
            elif data.data_type == common_pb2.StreamData.DataType.CHECKPOINT:
                # broadcast
                for output_partition_dispenser in partitions:
                    output_partition_dispenser.push_data(data)
            else:
                raise SystemExit(
                        "Failed: unknow data type({})".format(data.data_type))

    def start_standleton_process(self):
        proc = multiprocessing.Process(
                target=self._partitioning_data_and_carry_to_next_subtask,
                args=(
                    self.channel, 
                    self.output_endpoints,
                    self.subtask_name,
                    self.partition_idx))
        proc.daemon = True
        proc.start()
        return proc


class OutputPartitionDispenser(object):

    def __init__(self, endpoint: str, subtask_name: str, partition_idx: int):
        self.subtask_name = subtask_name
        self.partition_idx = partition_idx
        self.client = SubTaskClient()
        self.client.connect(endpoint)

    def push_data(self, data: serializator.SerializableStreamData) -> None:
        self.client.pushStreamData(
                subtask_pb2.PushStreamDataRequest(
                    from=self.subtask_name,
                    partition_idx=self.partition_idx,
                    data=data.instance_to_proto()))
