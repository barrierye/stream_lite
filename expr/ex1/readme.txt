1. 获取不做迁移的计算结果
  a. 在worker38启动JM和TM:
       python start_job_manager.py
       python start_task_manager.py conf/task_manager4step1.yaml
  b. 执行word-count程序:
       python submit_job.py conf/job.yaml step1
  c. 输出文件保存在sink.txt中

2. 获取做迁移的计算结果
  a. 在worker38启动JM和TM:
       python start_job_manager.py
       python start_task_manager.py conf/task_manager4step2_38.yaml
     在worker39启动TM:
       python start_task_manager.py conf/task_manager4step2_39.yaml
  b. 在worker38上执行word-count程序, 该程序会在1秒后触发迁移操作:
       python submit_job.py conf/job.yaml step2
  c. 输出文件保存在sink.txt中

3. 比较1和2得到的两个sink.txt有无差异
