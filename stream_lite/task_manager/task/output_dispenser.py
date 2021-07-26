#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import logging
from typing import List, Dict

from stream_lite.client import SubTaskClient

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
            output_endpoints: List[str]):
        """
        这里 output_endpoints 是按 partition_idx 顺序排列的
        """
        self.channel = output_channel
        self.output_endpoints = output_endpoints
        self._process = self.start_standleton_process()

    def push_data(self, data: serializator.SerializableStreamData) -> None:
        self.channel.put(data)

    def _partitioning_data_and_carry_to_next_subtask(self,
            input_channel: multiprocessing.Queue,
            output_endpoints: List[str]):
        partitions = [OutputPartitionDispenser(endpoint)
                for endpoint in output_endpoints]
        while True:
            data = input_channel.get()
            if data.data_type == common_pb2.StreamData.DataType.NORMAL:
                # partitioning
                pass
            elif data.data_type == common_pb2.StreamData.DataType.CHECKPOINT:
                # broadcast
                pass
            else:
                raise SystemExit(
                        "Failed: unknow data type({})".format(data.data_type))

    def start_standleton_process(self):
        proc = multiprocessing.Process(
                target=self._partitioning_data_and_carry_to_next_subtask,
                args=(self.channel, self.output_endpoints))
        proc.daemon = True
        proc.start()
        return proc


class OutputPartitionDispenser(object):

    def __init__(self, endpoint: str):
        self.client = SubTaskClient()
        self.client.connect(endpoint)

