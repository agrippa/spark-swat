#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 1 ]]; then
    echo 'usage: run.sh use-swat?'
    exit 1
fi

USE_SWAT=$1

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep imagenet | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    $HADOOP_HOME/bin/hdfs dfs -mkdir /imagenet
    $HADOOP_HOME/bin/hdfs dfs -mkdir /imagenet/input
    $HADOOP_HOME/bin/hdfs dfs -mkdir /imagenet/correct

    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATASETS/imagenet/5.repartition/part* \
        /imagenet/input/
    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATASETS/imagenet/6.correct/part* \
        /imagenet/correct/
fi

spark-submit --class SparkNN --jars ${SWAT_JARS} \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        run $1 $SPARK_DATASETS/imagenet/info \
        hdfs://$(hostname):54310/imagenet/input \
        hdfs://$(hostname):54310/imagenet/correct \
        1 3.0
