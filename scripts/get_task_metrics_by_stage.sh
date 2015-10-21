#!/bin/bash

set -e 

if [[ $# != 2 ]]; then
    echo 'usage: get_task_metrics_by_stage.sh log-file stage-num'
    exit 1
fi

LOG_FILE=$1
STAGE=$2

NTASKS=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | wc -l)
SUM_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/sum.py)
echo Stage $STAGE : $(echo $SUM_TASK_TIME / $NTASKS | bc -l) ms
