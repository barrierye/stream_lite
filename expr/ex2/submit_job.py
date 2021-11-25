#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-19
import logging
import inspect
import yaml
import time
import sys

from stream_lite import UserClient

_LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(
            format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M', level=logging.INFO)
    # for debug
    if len(sys.argv) != 3:
        _LOGGER.fatal("usage: python submit_job.py <conf.yaml> <stepN>")
        exit(1)
    conf_path = sys.argv[1]
    step = sys.argv[2]

    client = UserClient()
    client.connect('192.168.105.83:8970')
    
    if step == "step1":
        exit(1)
    elif step == "step2":
        jobid = client.submitJob(
                yaml_path=conf_path, 
                periodicity_checkpoint_interval_s=5,
                auto_migrate=True)
    elif step == "step3":
        jobid = client.submitJob(
                yaml_path=conf_path, 
                periodicity_checkpoint_interval_s=5,
                auto_migrate=False,
                enable_precopy=True)
        time.sleep(30)
        client.triggerMigrate(
                jobid=jobid,
                src_cls_name="SimpleSum",
                src_partition_idx=0,
                src_currency=1,
                target_task_manager_locate="TM_40",
                with_checkpoint_id=30)
    else:
        _LOGGER.fatal("stepN cannot be: {}".format(step))
        exit(-1)
