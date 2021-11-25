hostname=$1

function init_env() {
    tc qdisc del dev eth0 root
    tc qdisc add dev eth0 root handle 1:0 htb default 5
}

function set_latency() {
    if [ $# -ne 3 ]; then
        echo "Usage: set_latency <label> <to_ip> <latency>"
        return 1
    fi

    label=$1
    to_ip=$2
    latency=$3
    
    tc class add dev eth0 parent 1:0 classid 1:${label} htb rate 100kbit
    tc qdisc add dev eth0 parent 1:${label} netem delay ${latency}ms
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst ${to_ip} flowid 1:${label}
}

if [ "$hostname" == "38" ]; then
    init_env
    echo "give 192.168.105.39 20ms latency"
    set_latency 39 192.168.105.39 20
    echo "give 192.168.105.40 40ms latency"
    set_latency 40 192.168.105.40 40
    # echo "give 192.168.105.83 10ms latency"
    # set_latency 83 192.168.105.83 10
    echo "give 192.168.105.84 5000ms latency"
    set_latency 84 192.168.105.84 5000
elif [ "$hostname" == "84" ]; then
    init_env
    echo "give 192.168.105.39 40ms latency"
    set_latency 39 192.168.105.39 40
    echo "give 192.168.105.40 10ms latency"
    set_latency 40 192.168.105.40 10
    # echo "give 192.168.105.83 5000ms latency"
    # set_latency 83 192.168.105.83 5000
elif [ "$hostname" == "39" ]; then
    init_env
    echo "give 192.168.105.40 5000ms latency"
    set_latency 40 192.168.105.40 5000
    # echo "give 192.168.105.83 5000ms latency"
    # set_latency 83 192.168.105.83 5000
elif [ "$hostname" == "40" ]; then
    init_env
    # echo "give 192.168.105.83 10ms latency"
    # set_latency 83 192.168.105.83 10
else
    echo "error hostname: $hostname"
    exit 1
fi


