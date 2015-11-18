#!/bin/bash

set -e

APP=connected_components
NEXECUTORS=$(cat $SPARK_HOME/conf/slaves | wc -l)

# SPARK_LOG_FILE=$SWAT_HOME/logs/overall/$APP/spark
# SWAT_LOG_FILE=$SWAT_HOME/logs/overall/$APP/swat
SPARK_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/spark
SWAT_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/swat

# echo "" > $SPARK_LOG_FILE
# echo "" > $SWAT_LOG_FILE

for i in {1..1}; do
# ./run-hyperlink-graph.sh 1 false 2 2 2 >> $SPARK_LOG_FILE 2>&1
    ./run-hyperlink-graph.sh 1 true 2 2 2 >> $SWAT_LOG_FILE 2>&1
done
