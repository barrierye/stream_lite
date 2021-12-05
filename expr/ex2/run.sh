# bash ./init_84_net.sh 84 &

if [ $# -ne 1 ]; then
    echo "Usage: sh run.sh <stepN>"
    return 1
fi

step=$1

if [ "$step" == "step1" ]; then
    python submit_job.py null step1 &
    sleep 1
    python send_data.py
elif [ "$step" == "step2" ]; then
    python submit_job.py conf/job4step2.yaml step2 &
    sleep 1
    python send_data.py
else
    echo "error step: $step"
    exit 1
fi
