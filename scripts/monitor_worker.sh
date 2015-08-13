#!/bin/bash

# set -e

if [[ $# != 2 ]]; then
    echo usage: monitor_worker.sh node period
    exit 1
fi

echo CPU, MEM
while :; do
    ssh $1 "ps -A -o %cpu,%mem,pid,args | grep java | grep -v grep"
    echo
    ssh $1 nvidia-smi
    echo
#     PID=$(ssh $1 "ps aux | grep java | grep Worker | grep -v grep | awk '{ print \$2 }' ")
#     ssh $1 "ps -p $PID -o %cpu,%mem | tail -n 1"
    sleep $2
done
