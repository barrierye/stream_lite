#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-26
import multiprocessing
import logging
from typing import List, Dict
import queue

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
        self.partitions = [InputPartitionReceiver(endpoint)
                for endpoint in range(input_endpoints)]

    def recv_data(self, partition_idx: int, 
            data: serializator.SerializableStreamData):
        pass


class InputPartitionReceiver(object):

    def __init__(self, endpoint: str):
        self.queue = queue.Queue()
