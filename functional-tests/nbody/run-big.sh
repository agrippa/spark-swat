#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 2 ]]; then
    echo usage: run.sh niters use-swat?
    exit 1
fi

DATA_DIR=$SPARK_DATA/nbody

for TEST in $(ls $DATA_DIR); do
    echo $TEST

    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-pairs
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-pairs

    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
    ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/points/input.* /input/

    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-pairs
    ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/pairs/input.* /input-pairs/

    spark-submit --class SparkNBody --master spark://localhost:7077 --jars ${SWAT_JARS} \
            ${SCRIPT_DIR}/target/sparknbody-0.0.0.jar convert \
            hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted \
            hdfs://$(hostname):54310/input-pairs \
            hdfs://$(hostname):54310/converted-pairs

    spark-submit --class SparkNBody --jars ${SWAT_JARS} \
            --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
            --master spark://localhost:7077 ${SCRIPT_DIR}/target/sparknbody-0.0.0.jar \
            run $1 hdfs://$(hostname):54310/converted hdfs://$(hostname):54310/converted-pairs $2
done
