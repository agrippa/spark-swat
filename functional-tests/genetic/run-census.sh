#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 8 ]]; then
    echo usage: run.sh niters min-n-clusters max-n-clusters population-size use-swat? heaps-per-device n-inputs n-outputs
    exit 1
fi

ITERS=$1
MIN_N_CLUSTERS=$2
MAX_N_CLUSTERS=$3
POPULATION_SIZE=$4
USE_SWAT=$5
HEAPS_PER_DEVICE=$6
NINPUTS=$7
NOUTPUTS=$8

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=70000 -Dswat.heap_size=83886080 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true"

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep census-data | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /census-data
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/census/2.split/part* /census-data/
        # $SPARK_DATASETS/census/3.dup/part* /census-data/
fi
# --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19 -Xloggc:/tmp/SWAT.log -verbose:gc" \
# --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails" \

spark-submit --class SparkGenetic --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --master spark://localhost:7077 ${SCRIPT_DIR}/target/sparkgenetic-0.0.0.jar \
        $ITERS $MIN_N_CLUSTERS $MAX_N_CLUSTERS $POPULATION_SIZE \
        hdfs://$(hostname):54310/census-data $USE_SWAT
