#!/bin/bash

if [[ $# != 2 ]]; then
    echo usage: profile_info_from_latest.sh sub-folder lbl
    exit 1
fi

SPARK_WORK_DIR=/scratch/jmg3/spark-work

SUBFOLDER=$1
LBL=$2

FOLDER=$(ls -lrt $SPARK_WORK_DIR | tail -n 1 | awk '{ print $9 }')
cat $SPARK_WORK_DIR/$FOLDER/$SUBFOLDER/stderr | grep "SWAT PROF" | grep $LBL | awk '{ print $5 }' | python ~/sum.py
