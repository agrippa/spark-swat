#!/bin/bash

if [[ $# != 3 ]]; then
    echo 'usage: get_task_metrics.sh log-file src-file src-line'
    exit 1
fi

LOG_FILE=$1
SRC_FILE=$2
SRC_LINE=$3
SRC_LOC=$2:$3

for STAGE in $(cat $LOG_FILE | grep "$SRC_LOC" | grep "Submitting Stage" | awk '{ print $7 }'); do
    NTASKS=$(cat tmp.swat | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | wc -l)
    SUM_TASK_TIME=$(cat tmp.swat | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/sum.py)
    echo Stage $STAGE : $(echo $SUM_TASK_TIME / $NTASKS | bc -l) ms
done
