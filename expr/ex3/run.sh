if [ $# -ne 1 ]; then
    echo "Usage: sh run.sh <stepN>"
    return 1
fi

step=$1

if [ "$step" == "step1" ]; then
    python submit_job.py conf/job4step2.yaml step1
elif [ "$step" == "step2" ]; then
    python submit_job.py conf/job4step2.yaml step2
else
    echo "error step: $step"
    exit 1
fi
