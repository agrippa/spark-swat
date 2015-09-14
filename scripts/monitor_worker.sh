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
    sleep $2
done
