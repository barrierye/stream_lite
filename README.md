StreamLite



## Quick Start

```shell
# start job manager
python start_job_manager.py

# start task manager
python start_task_manager.py ./conf/task_manager1.yaml

# start job
python start_user_client.py
```

## TODO

1. 只支持单 Job 运行
2. CheckpointHelper 会持续执行，即使任务已经结束
