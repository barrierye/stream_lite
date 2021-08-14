#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import logging
import sys

from stream_lite import TaskManager

_LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(
            format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M', 
            level=logging.INFO)
    # for debug
    if len(sys.argv) != 2:
        _LOGGER.fatal("usage: python start_task_manager.py <conf.yaml>")
        exit(1)
    task_manager_conf = sys.argv[1]
    server = TaskManager(task_manager_conf)
    server.run()
