1. ?

2. 获取做迁移的计算结果
  a. 在worker88启动JM和TM:
       python start_job_manager.py
     在worker38启动TM:
       python start_task_manager.py conf/task_manager38.yaml
     在worker39启动TM:
       python start_task_manager.py conf/task_manager39.yaml
     在worker40启动TM:
       python start_task_manager.py conf/task_manager40.yaml
     在worker84启动TM:
       python start_task_manager.py conf/task_manager84.yaml
  b. 提交word-count程序, 该程序自动触发迁移操作:
       python submit_job.py conf/job4step2.yaml step2
  c. 修改worker84的网络延迟:
       bash ./init_84_net.sh 84
  c. 输出文件保存在sink.txt中

3. 比较1和2得到的两个sink.txt有无差异
