#!/bin/bash

set -e

if [[ $# != 3 ]]; then
    echo 'usage: get_task_metrics_by_line.sh log-file src-file src-line'
    exit 1
fi

LOG_FILE=$1
SRC_FILE=$2
SRC_LINE=$3
SRC_LOC=$2:$3

# 15/09/24 20:43:09 INFO DAGScheduler: Final stage: Stage 15(count at SparkKMeans.scala:118)
for STAGE in $(cat $LOG_FILE | grep "$SRC_LOC" | grep "Final stage" | awk '{ print $8 }' | cut -f1 -d"("); do
    NTASKS=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | wc -l)
    SUM_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/sum.py)
    echo Stage $STAGE : $(echo $SUM_TASK_TIME / $NTASKS | bc -l) ms
done
