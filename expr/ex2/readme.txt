1. 获取不做迁移的计算结果
  a. 在worker38启动JM和TM:
       python start_job_manager.py
       python start_task_manager.py conf/task_manager4step1.yaml
  b. 提交word-count程序:
       python submit_job.py conf/job4step1.yaml step1
  c. 输出文件保存在sink.txt中

2. 获取做迁移的计算结果
  a. 在worker38启动JM和TM:
       python start_job_manager.py
       python start_task_manager.py conf/task_manager4step2_38.yaml
     在worker39启动TM:
       python start_task_manager.py conf/task_manager4step2_39.yaml
  b. 提交word-count程序(一部分计算在worker39), 该程序自动触发迁移操作(迁移回worker38):
       python submit_job.py conf/job4step2.yaml step2
  c. 输出文件保存在sink.txt中

3. 比较1和2得到的两个sink.txt有无差异
