#!/bin/bash

set -e

if [[ $# != 1 ]]; then
    echo get_swat_task_metrics_from_log_file.sh log-file
    exit 1
fi

LOG_FILE=$1

APP_ID=$(cat $LOG_FILE | grep "Connected to Spark cluster with app ID" | awk '{ print $12 }')
STAGES=$(cat /scratch/jmg3/spark-work/$APP_ID/0/stderr | grep "stage = " | awk '{ print $13 }' | sort | uniq)
for STAGE in $STAGES; do
    STAGE=${STAGE:0:${#STAGE}-1}
    get_task_metrics_by_stage.sh -i $LOG_FILE -s $STAGE
done

