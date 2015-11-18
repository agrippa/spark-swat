#!/bin/bash

set -e

APP=kmeans
NEXECUTORS=$(cat $SPARK_HOME/conf/slaves | wc -l)

# SPARK_LOG_FILE=$SWAT_HOME/logs/overall/$APP/spark
# SWAT_LOG_FILE=$SWAT_HOME/logs/overall/$APP/swat
SPARK_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/spark
SWAT_LOG_FILE=$SWAT_HOME/logs/scalability/$APP/$NEXECUTORS/swat

# echo "" > $SPARK_LOG_FILE
# echo "" > $SWAT_LOG_FILE

for i in {1..1}; do
    # usage: run.sh niters ncenters use-swat? heaps-per-device n-inputs n-outputs
# ./run-census.sh 1 2000 false 2 2 2 >> $SPARK_LOG_FILE 2>&1
    ./run-census.sh 1 2000 true 2 2 2 >> $SWAT_LOG_FILE 2>&1
done
