#!/bin/bash

set -e 

if [[ $# != 1 ]]; then
    echo 'usage: extract_stage_times.sh main-log-file'
    exit 1
fi


LOG_FILE=$(readlink -e $1)
LOG_DIR=$(dirname $LOG_FILE)
JUST_FILE=$(basename $1)

AWK_STAGE_STR=
if [[ $JUST_FILE == spark ]]; then
    AWK_STAGE_STR='{ print $7 }'
elif [[ $JUST_FILE == swat ]]; then
    AWK_STAGE_STR='{ print $15 }'
else
    echo 'Expected to be passed either a "spark" or "swat" file'
    exit 1
fi

EXPECTED_STAGES=
for APP in $(cat $LOG_FILE | grep "Connected to" | awk '{ print $12 }'); do
    echo $APP
    if [[ ! -d $LOG_DIR/$APP ]]; then
        echo Seem to be missing log directory $LOG_DIR/$APP
        exit 1
    fi
    STAGES=$(for JOB_FILE in $(find $LOG_DIR/$APP -name "stderr"); do
                 cat $JOB_FILE | grep Thread | awk "$AWK_STAGE_STR" | sort | uniq
             done)
    # Possible duplicates from different log files
    UNIQ_STAGES=$(for STAGE in $STAGES; do
                      echo $STAGE
                  done | sort | uniq)
    FINAL_STAGES=$(for STAGE in $UNIQ_STAGES; do echo ${STAGE:0:${#STAGE}-1}; done | sort -n)

    if [[ -z $EXPECTED_STAGES ]]; then
        EXPECTED_STAGES=$FINAL_STAGES
    else
        if [[ "$EXPECTED_STAGES" != "$FINAL_STAGES" ]]; then
            echo "Mismatch in stages"
            echo "Expected $EXPECTED_STAGES"
            echo "Got $FINAL_STAGES"
            exit 1
        fi
    fi
done

echo
echo -e "STAGE\tMEDIAN\tMEAN"
for STAGE in $(echo $EXPECTED_STAGES); do
    MEDIAN=$(cat $LOG_FILE | grep Finished | grep "in stage $STAGE.0" | awk '{ print $14 }' | python ~/median.py)
    MEAN=$(cat $LOG_FILE | grep Finished | grep "in stage $STAGE.0" | awk '{ print $14 }' | python ~/mean.py)
    echo -e "$STAGE\t$MEDIAN\t$MEAN"
done

