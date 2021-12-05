# bash ./init_84_net.sh 84 &
python submit_job.py null step1 &
sleep 1
python send_data.py
