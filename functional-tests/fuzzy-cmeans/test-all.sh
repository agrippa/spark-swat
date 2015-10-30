#!/bin/bash

set -e

SPARK_LOG_FILE=$SWAT_HOME/logs/overall/fuzzy/spark
SWAT_LOG_FILE=$SWAT_HOME/logs/overall/fuzzy/swat

echo "" > $SPARK_LOG_FILE
echo "" > $SWAT_LOG_FILE

for i in {1..10}; do
    # usage: run.sh niters ncenters use-swat? heaps-per-device n-inputs n-outputs
    ./run-census.sh 2 100 true 2 2 2 >> $SWAT_LOG_FILE 2>&1
    ./run-census.sh 2 100 false 2 2 2 >> $SPARK_LOG_FILE 2>&1
done
