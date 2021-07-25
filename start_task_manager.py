#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import logging

from stream_lite import TaskManager

if __name__ == '__main__':
    logging.basicConfig(
            format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M', 
            level=logging.DEBUG)
    # for debug
    server = TaskManager("conf/task_manager1.yaml", 8971)
    server.run()
