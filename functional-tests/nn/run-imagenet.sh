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

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep imagenet | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    $HADOOP_HOME/bin/hdfs dfs -mkdir /imagenet
    $HADOOP_HOME/bin/hdfs dfs -mkdir /imagenet/input

#     $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATASETS/imagenet/5.repartition/part* \
#         /imagenet/input/
#     $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATASETS/imagenet/7.multiply_again/part* \
#         /imagenet/input/
    $HADOOP_HOME/bin/hdfs dfs -put $SPARK_DATASETS/imagenet/8.subset/part* \
        /imagenet/input/
fi

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /imagenet/correct

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=10000 -Dswat.heap_size=41943040 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true"
# -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true -Dswat.kernels_dir=/home/jmg3/spark-swat/functional-tests/nn/kernels"

# NUM_EXECUTORS=$(scontrol show hostname | wc -l)
NUM_EXECUTORS=3
NUM_EXECUTORS=$(echo $NUM_EXECUTORS - 1 | bc)
echo $NUM_EXECUTORS executors
CORES_PER_EXECUTOR=$(cat /proc/cpuinfo | grep processor | wc -l)
echo $CORES_PER_EXECUTOR cores per executor
PARTITIONS=$(echo $NUM_EXECUTORS \* $CORES_PER_EXECUTOR \* 2 | bc)
echo $PARTITIONS total partitions

spark-submit --class SparkNN --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --conf "spark.driver.maxResultSize=4096M" \
        --conf "spark.storage.memoryFraction=0.1" \
        --conf "spark.executor.memory=35g" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        run $1 $SPARK_DATASETS/imagenet/info \
        hdfs://$(hostname):54310/imagenet/input \
        hdfs://$(hostname):54310/imagenet/correct \
        $ITERS 3.0 $PARTITIONS
