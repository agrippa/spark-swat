#!/bin/bash

set -e

SPARK_LOG_FILE=$SWAT_HOME/logs/overall/nn/spark
SWAT_LOG_FILE=$SWAT_HOME/logs/overall/nn/swat

echo "" > $SPARK_LOG_FILE
echo "" > $SWAT_LOG_FILE

for i in {1..5}; do
    # usage: run.sh use-swat? iters n-inputs n-outputs heaps-per-device
    ./run-imagenet.sh true 1 2 2 2 >> $SWAT_LOG_FILE 2>&1
    ./run-imagenet.sh false 1 2 2 2 >> $SPARK_LOG_FILE 2>&1
done
