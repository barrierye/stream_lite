ps -ef | grep "start_task_manager" | grep -v grep| awk '{print $2}' | xargs kill
