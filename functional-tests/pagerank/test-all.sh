#!/bin/bash

set -e

# SPARK_LOG_FILE=$SWAT_HOME/logs/overall/pagerank/spark
# SWAT_LOG_FILE=$SWAT_HOME/logs/overall/pagerank/swat
SWAT_LOG_FILE=$SWAT_HOME/logs/balance/pagerank/4/swat
SPARK_LOG_FILE=$SWAT_HOME/logs/balance/pagerank/4/spark

echo "" > $SPARK_LOG_FILE
echo "" > $SWAT_LOG_FILE

for i in {1..3}; do
    ./run-hyperlink-graph.sh true 1 2 2 2 >> $SWAT_LOG_FILE 2>&1
    ./run-hyperlink-graph.sh false 1 2 2 2 >> $SPARK_LOG_FILE 2>&1
done
