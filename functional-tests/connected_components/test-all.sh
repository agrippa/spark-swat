#!/bin/bash

set -e

SPARK_LOG_FILE=$SWAT_HOME/logs/connected_components/spark
SWAT_LOG_FILE=$SWAT_HOME/logs/connected_components/swat

echo "" > $SPARK_LOG_FILE
echo "" > $SWAT_LOG_FILE

for i in {1..10}; do
    echo $i
    ./run-hyperlink-graph.sh 1 false 2 2 2 >> $SPARK_LOG_FILE 2>&1
    ./run-hyperlink-graph.sh 1 true 2 2 2 >> $SWAT_LOG_FILE 2>&1
done
