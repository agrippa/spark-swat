#!/bin/bash

set -e

APP=pagerank
NEXECUTORS=$(cat $SPARK_HOME/conf/slaves | wc -l)

# SPARK_LOG_FILE=$SWAT_HOME/logs/overall/pagerank/spark
# SWAT_LOG_FILE=$SWAT_HOME/logs/overall/pagerank/swat
# SPARK_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/spark
# SWAT_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/swat

SPARK_LOG_FILE=spark.log
SWAT_LOG_FILE=swat.log

# echo "" > $SPARK_LOG_FILE
# echo "" > $SWAT_LOG_FILE

for i in {1..2}; do
    # ./run-hyperlink-graph.sh true 1 2 2 2 >> $SWAT_LOG_FILE 2>&1
    ./run-hyperlink-graph.sh false 1 2 2 2 >> $SPARK_LOG_FILE 2>&1
done
