#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-27
import random
import logging

from stream_lite.network import serializator

_LOGGER = logging.getLogger(__name__)


class PartitionerBase(object):

    def __init__(self):
        pass

    @staticmethod
    def partitioning(
            data: serializator.SerializableRecord,
            partition_num: int) -> int:
        raise NotImplementedError("Failed: function not implemented")


class KeyPartitioner(PartitionerBase):

    def __init__(self):
        super(KeyPartitioner, self).__init__()

    @staticmethod
    def partitioning(
            data: serializator.SerializableRecord,
            partition_num: int) -> int:
        partition_key = data.partition_key
        return partition_key % partition_num


class TimestampPartitioner(PartitionerBase):

    def __init__(self):
        super(TimestampPartitioner, self).__init__()

    @staticmethod
    def partitioning(
            data: serializator.SerializableRecord,
            partition_num: int) -> int:
        timestamp = data.timestamp
        return timestamp % partition_num


class RandomPartitioner(PartitionerBase):

    def __init__(self):
        super(TimestampPartitioner, self).__init__()

    @staticmethod
    def partitioning(
            data: serializator.SerializableRecord,
            partition_num: int) -> int:
        random_num = random.randint(0, partition_num - 1)
        return random_num % partition_num
