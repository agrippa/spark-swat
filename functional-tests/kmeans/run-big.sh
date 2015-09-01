#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 2 ]]; then
    echo usage: run.sh niters use-swat?
    exit 1
fi

DATA_DIR=$SPARK_DATA/kmeans

for TEST in $(ls $DATA_DIR); do
    echo $TEST

    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
    ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted

    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
    ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/input.* /input/

    spark-submit --class SparkKMeans --master spark://localhost:7077 --jars ${SWAT_JARS} \
            ${SCRIPT_DIR}/target/sparkkmeans-0.0.0.jar convert \
            hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted

    spark-submit --class SparkKMeans --jars ${SWAT_JARS} \
            --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
            --master spark://localhost:7077 ${SCRIPT_DIR}/target/sparkkmeans-0.0.0.jar \
            run ${DATA_DIR}/$TEST/info $1 hdfs://$(hostname):54310/converted $2
done
