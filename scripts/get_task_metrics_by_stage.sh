#!/bin/bash

set -e 

STAGES=
LOG_FILE=
VERBOSE=0

while getopts "i:s:hv" opt; do
    case $opt in
        i)
            LOG_FILE=$OPTARG
            ;;
        s)
            STAGES="$STAGES $OPTARG"
            ;;
        h)
            echo 'usage: get_task_metrics_by_stage.sh -i log-file -s stage [-h] [-v]'
            exit 1
            ;;
        v)
            VERBOSE=1
            ;;
        \?)
            echo "unrecognized option -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done

if [[ -z $LOG_FILE ]]; then
    echo Missing log file
    exit 1
fi
if [[ -z $STAGES ]]; then
    echo Missing stages
    exit 1
fi

for STAGE in $STAGES; do
    NTASKS=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | wc -l)
    SUM_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/sum.py)
    MIN_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/min.py)
    MAX_TASK_TIME=$(cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }' | python ~/max.py)
    TASK_LABEL=$(cat $LOG_FILE | grep "Stage $STAGE " | grep finished | awk '{ print $9 }')
    echo $TASK_LABEL
    TASK_LABEL=${TASK_LABEL:0:${#TASK_LABEL}-1}
    if [[ $NTASKS == 0 ]]; then
        echo Stage $STAGE : No tasks found
    else
        echo "Stage $STAGE : $TASK_LABEL : $(echo $SUM_TASK_TIME / $NTASKS | bc -l) ms (max: $MAX_TASK_TIME, min: $MIN_TASK_TIME)"
        if [[ $VERBOSE == 1 ]]; then
            cat $LOG_FILE | grep "Finished task" | grep "in stage ${STAGE}.0" | awk '{ print $14 }'
            echo
        fi
    fi
done
