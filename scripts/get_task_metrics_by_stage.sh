#!/bin/bash

set -e 

if [[ $# < 2 ]]; then
    echo 'usage: get_task_metrics_by_stage.sh log-file stage-num1 stage-num2 ...'
    exit 1
fi

LOG_FILE=$1
shift
STAGES="$@"

for STAGE in $STAGES; do
    NTASKS=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | wc -l)
    SUM_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/sum.py)
    if [[ $NTASKS == 0 ]]; then
        echo Stage $STAGE : No tasks found
    else
        echo Stage $STAGE : $(echo $SUM_TASK_TIME / $NTASKS | bc -l) ms
    fi
done
