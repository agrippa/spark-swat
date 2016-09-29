#!/bin/bash

set -e

if [[ $# != 5 ]]; then
    echo 'usage: run-hyperlink-graph.sh use-swat? iters heaps-per-device n-inputs n-outputs'
    exit 1
fi

USE_SWAT=$1
ITERS=$2
HEAPS_PER_DEVICE=$2
NINPUTS=$3
NOUTPUTS=$4

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep hyperlink-links-filtered | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /hyperlink-links-filtered
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/3.renormalize/part* /hyperlink-links-filtered
fi

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep docs-filtered | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /docs-filtered
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/3.docranks_again/part* /docs-filtered
fi

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=100000 -Dswat.heap_size=3000000 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE -Dswat.print_kernel=true"

spark-submit --class SparkPageRank --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --conf "spark.executor.memory=30g" \
        --master spark://localhost:7077 $SCRIPT_DIR/target/sparkpagerank-0.0.0.jar \
        run $ITERS hdfs://$(hostname):54310/hyperlink-links-filtered \
        hdfs://$(hostname):54310/docs-filtered $USE_SWAT
