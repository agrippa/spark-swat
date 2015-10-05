#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 1 ]]; then
    echo 'usage: run.sh use-swat?'
    exit 1
fi

USE_SWAT=$1

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep mnist | wc -l)
if [[ $INPUT_EXISTS == 0 ]]; then
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist/input
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist/correct

    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATA/nn/training/input.* \
        /mnist/input/
    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATA/nn/correct/input.* \
        /mnist/correct/

    $HADOOP_HOME/bin/hdfs dfs -rm -f -r /mnist-converted

    spark-submit --class SparkNN --master spark://localhost:7077 \
        --jars ${SWAT_JARS} $SCRIPT_DIR/target/nn-0.0.0.jar convert \
        hdfs://$(hostname):54310/mnist/input \
        hdfs://$(hostname):54310/mnist-converted/input \
        hdfs://$(hostname):54310/mnist/correct \
        hdfs://$(hostname):54310/mnist-converted/correct
fi

spark-submit --class SparkNN --jars ${SWAT_JARS} \
        --master spark://localhost:7077 \
        --conf "spark.executor.extraJavaOptions=-Dswat.input_chunking=10000 -Dswat.cl_local_size=128" \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        run $1 $SPARK_DATA/nn/info \
        hdfs://$(hostname):54310/mnist-converted/input \
        hdfs://$(hostname):54310/mnist-converted/correct \
        1 3.0
