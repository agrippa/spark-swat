#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 6 ]]; then
    echo usage: run.sh niters ncenters use-swat? heaps-per-device n-inputs n-outputs
    exit 1
fi

ITERS=$1
CENTERS=$2
USE_SWAT=$3
HEAPS_PER_DEVICE=$4
NINPUTS=$5
NOUTPUTS=$6

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=100000 -Dswat.heap_size=146800640 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true"

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep census-data | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /census-data
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/census/3.dup/part* /census-data/
fi
# --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19 -Xloggc:/tmp/SWAT.log -verbose:gc" \
# --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails" \

spark-submit --class SparkKMeans --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --master spark://localhost:7077 ${SCRIPT_DIR}/target/sparkkmeans-0.0.0.jar \
        run $CENTERS $ITERS hdfs://$(hostname):54310/census-data $USE_SWAT
