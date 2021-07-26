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
    SubTask   ->   channel   ->   Dispenser    - PartitionDispenser   ->  SubTask
                               (SubTaskClient) \ PartitionDispenser
    """

    def __init__(self, 
            output_channel: multiprocessing.Queue,
            output_endpoints: List[str]):
        """
        这里 output_endpoints 是按 partition_idx 顺序排列的
        """
        self.channel = output_channel
        self.partitions = [OutputPartitionDispenser(endpoint) 
                for endpoint in range(output_endpoints)]


class OutputPartitionDispenser(object):

    def __init__(self, endpoint: str):
        self.client = SubTaskClient()
        self.client.connect(endpoint)
