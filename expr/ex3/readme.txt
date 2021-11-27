1. ?

2. 本毕设不做优化
  a. word-count
    1) 在worker83启动JM:
         python start_job_manager.py
       在worker38启动TM:
         python start_task_manager.py conf/task_manager38.yaml
       在worker39启动TM:
         python start_task_manager.py conf/task_manager39.yaml
       在worker40启动TM:
         python start_task_manager.py conf/task_manager40.yaml
       在worker84启动TM:
         python start_task_manager.py conf/task_manager84.yaml
    2) 提交word-count程序, 该程序自动触发迁移操作:
         python submit_job.py conf/job4step2.yaml step2
    3) 修改worker84的网络延迟:
         bash ./init_84_net.sh 84
    4) 输出文件保存在sink.txt中
  b. YOLOv3
    1) 安装
       python3 -m pip install paddle_serving_app
       python3 -m paddle_serving_app.package --get_model ocr_rec
       python3 -m paddle_serving_app.package --get_model ocr_det
       tar -xzvf ocr_rec.tar.gz
       tar -xzvf ocr_det.tar.gz

3. 本毕设做优化（自动预迁移状态+手动迁移）
