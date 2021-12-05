ps -ef | grep "start_task_manager" | grep -v grep| awk '{print $2}' | xargs kill
ps -ef | grep "send_data" | grep -v grep| awk '{print $2}' | xargs kill
