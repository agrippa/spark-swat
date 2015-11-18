#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 5 ]]; then
    echo 'usage: run.sh use-swat? iters n-inputs n-outputs heaps-per-device'
    exit 1
fi

USE_SWAT=$1
ITERS=$2
NINPUTS=$3
NOUTPUTS=$4
HEAPS_PER_DEVICE=$5

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep mnist | wc -l)
if [[ $INPUT_EXISTS == 0 ]]; then
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist/input
#     $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist/correct

    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATA/nn/training/input.* \
        /mnist/input/
#     $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATA/nn/correct/input.* \
#             /mnist/correct/

    $HADOOP_HOME/bin/hdfs dfs -rm -f -r /mnist-converted
    $HADOOP_HOME/bin/hdfs dfs -mkdir /mnist-converted

    spark-submit --class SparkNN --master spark://localhost:7077 \
        --jars ${SWAT_JARS} $SCRIPT_DIR/target/nn-0.0.0.jar convert \
        hdfs://$(hostname):54310/mnist/input \
        hdfs://$(hostname):54310/mnist-converted/input
fi

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /mnist/correct

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=128 \
              -Dswat.input_chunking=2000 -Dswat.heap_size=62914560 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true"

NUM_EXECUTORS=$(scontrol show hostname | wc -l)
NUM_EXECUTORS=$(echo $NUM_EXECUTORS - 1 | bc)
echo $NUM_EXECUTORS executors
CORES_PER_EXECUTOR=$(cat /proc/cpuinfo | grep processor | wc -l)
echo $CORES_PER_EXECUTOR cores per executor
PARTITIONS=$(echo $NUM_EXECUTORS \* $CORES_PER_EXECUTOR \* 2 | bc)
echo $PARTITIONS total partitions

spark-submit --class SparkNN --jars ${SWAT_JARS} \
        --master spark://localhost:7077 --conf "$SWAT_OPTIONS" \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        run $USE_SWAT $SPARK_DATA/nn/info \
        hdfs://$(hostname):54310/mnist-converted/input \
        hdfs://$(hostname):54310/mnist-converted/correct \
        $ITERS 3.0 $PARTITIONS
