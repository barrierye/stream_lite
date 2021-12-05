# bash ./init_84_net.sh 84 &
python submit_job.py null step1 &
sleep 1
rm result.txt
python send_data.py &
tail -f result.txt
