#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import os
import multiprocessing
import threading
import logging
from typing import List, Dict
import queue

from stream_lite.proto import common_pb2

import stream_lite.config
from stream_lite.network import serializator


_LOGGER = logging.getLogger(__name__)


class InputReceiver(object):
    """
    PartitionDispenser   \
    PartitionDispenser   -    subtask   ->   InputReceiver   ->   channel
    PartitionDispenser   /            (遇到 event 阻塞，类似 Gate)
    """

    def __init__(self, 
            input_channel: multiprocessing.Queue,
            input_endpoints: List[str]):
        self.channel = input_channel
        self.event_barrier = multiprocessing.Barrier(
                parties=len(input_endpoints))
        self.partitions = []
        for endpoint in input_endpoints:
            input_partition_receiver = InputPartitionReceiver(
                    self.channel, endpoint, self.event_barrier)
            input_partition_receiver.start_standleton_process(
                    is_process=stream_lite.config.IS_PROCESS)
            self.partitions.append(input_partition_receiver)

    def recv_data(self, partition_idx: int, 
            record: common_pb2.Record) -> None:
        self.partitions[partition_idx].recv_data(record)


class InputPartitionReceiver(object):

    def __init__(self, 
            channel: multiprocessing.Queue, 
            endpoint: str,
            event_barrier: multiprocessing.Barrier):
        self.queue = multiprocessing.Queue()
        self.channel = channel
        self.event_barrier = event_barrier
        self._process = None

    def recv_data(self, record: common_pb2.Record) -> None:
        self.queue.put(record)
    
    def _prase_data_and_carry_to_channel(self, 
            input_queue: multiprocessing.Queue,
            output_channel: multiprocessing.Queue,
            event_barrier: multiprocessing.Barrier):
        try:
            self._inner_prase_data_and_carry_to_channel(
                    input_queue, output_channel, event_barrier)
        except Exception as e:
            _LOGGER.critical(
                    "Failed: run input_partition_receiver failed ({})".format(e), exc_info=True)
            os._exit(-1)

    def _inner_prase_data_and_carry_to_channel(self,
            input_queue: multiprocessing.Queue,
            output_channel: multiprocessing.Queue,
            event_barrier: multiprocessing.Barrier):
        need_barrier_datatype = [
                common_pb2.Record.DataType.FINISH,
                common_pb2.Record.DataType.CHECKPOINT,
                common_pb2.Record.DataType.MIGRATE]
        while True:
            proto_data = input_queue.get()
            seri_data = serializator.SerializableRecord.from_proto(proto_data)
            if seri_data.data_type in need_barrier_datatype:
                order = event_barrier.wait()
                if order == 0:
                    # only order == 0 push event to output channel
                    output_channel.put(seri_data)
                    event_barrier.reset()
            else:
                output_channel.put(seri_data)

    def start_standleton_process(self, is_process):
        """
        起一个独立进程，不断处理数据到 channel 中
        """
        if self._process is not None:
            raise SystemExit("Failed: process already running")
        if is_process:
            self._process = multiprocessing.Process(
                    target=self._prase_data_and_carry_to_channel,
                    args=(self.queue, self.channel, 
                        self.event_barrier), 
                    daemon=True)
        else:
            self._process = threading.Thread(
                    target=self._prase_data_and_carry_to_channel,
                    args=(self.queue, self.channel,
                        self.event_barrier))
        self._process.start()
