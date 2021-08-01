#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import logging
import inspect
import yaml
import time

from stream_lite import UserClient

if __name__ == '__main__':
    logging.basicConfig(
            format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M', level=logging.INFO)
    conf_path = "conf/user.yaml"
    # for debug
    client = UserClient()
    client.connect('0.0.0.0:8970')
    jobid = client.submitJob(conf_path)
    time.sleep(1)
    client.triggerCheckpoint(jobid, cancel_job=True)
