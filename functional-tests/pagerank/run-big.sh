#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 2 ]]; then
    echo usage: run.sh niters use-swat?
    exit 1
fi

DATA_DIR=$SPARK_DATA/pagerank

for TEST in $(ls $DATA_DIR); do
    echo $TEST

    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f /input-docs
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-docs

    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
    ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/links/input.* /input-links/
    ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/docs /input-docs

    spark-submit --class SparkPageRank --master spark://localhost:7077 \
            --jars ${SWAT_JARS} ${SCRIPT_DIR}/target/sparkpagerank-0.0.0.jar convert \
            hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/converted-links \
            hdfs://$(hostname):54310/input-docs hdfs://$(hostname):54310/converted-docs

    spark-submit --class SparkPageRank --jars ${SWAT_JARS} \
            --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
            --master spark://localhost:7077 $SCRIPT_DIR/target/sparkpagerank-0.0.0.jar \
            run $1 hdfs://$(hostname):54310/converted-links hdfs://$(hostname):54310/converted-docs $2
