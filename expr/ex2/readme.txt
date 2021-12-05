1. flink-link
  a. 在worker83启动JM:
       python start_job_manager.py
  b. 在worker38启动TM:
       python start_task_manager.py conf/task_manager38.yaml
     在worker39启动TM:
       python start_task_manager.py conf/task_manager39.yaml
     在worker40启动TM:
       python start_task_manager.py conf/task_manager40.yaml
     在worker84启动TM:
       python start_task_manager.py conf/task_manager84.yaml
  c. 提交word-count程序, 一段时间后停止并将状态转移到目标机器。然后重启:
       sh run.sh

2. 本毕设
