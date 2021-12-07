if [ $# -ne 1 ]; then
    echo "Usage: bash init_bandwidth.sh <bandwidth>"
    return 1
fi

bandwidth=$1 #kbps
wondershaper eth0 $bandwidth $bandwidth
