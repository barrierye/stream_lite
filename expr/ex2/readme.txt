1. flink-link
  a. 在worker83启动JM:
       sh init_env.sh
       python start_job_manager.py
  b. 在worker38启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager38.yaml
     在worker39启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager39.yaml
     在worker40启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager40.yaml
     在worker84启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager84.yaml
  c. 提交word-count程序, 一段时间后停止并将状态转移到目标机器。然后重启:
       sh init_env.sh
       bash run.sh step1
  d. 响应延迟在作业提交机器的result.txt，迁移触发时间在JM日志中

2. 本毕设
  a. 在worker83启动JM:
       sh init_env.sh
       python start_job_manager.py
  b. 在worker38启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager38.yaml
     在worker39启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager39.yaml
     在worker40启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager40.yaml
     在worker84启动TM:
       sh init_env.sh
       python start_task_manager.py conf/task_manager84.yaml
  c. 提交word-count程序, 一段时间后停止并将状态转移到目标机器。然后重启:
       sh init_env.sh
       bash run.sh step2
  d. 响应延迟在作业提交机器的result.txt，迁移触发时间在JM日志中
