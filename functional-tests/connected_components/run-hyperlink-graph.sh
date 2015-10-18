#!/bin/bash

set -e

if [[ $# != 4 ]]; then
    echo 'usage: run-hyperlink-graph.sh use-swat? heaps-per-device n-inputs n-outputs'
    exit 1
fi

USE_SWAT=$1
HEAPS_PER_DEVICE=$2
NINPUTS=$3
NOUTPUTS=$4

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep hyperlink-graph-links | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /hyperlink-graph-links
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/1.normalized/part* /hyperlink-graph-links
fi

# --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=100000 -Dswat.heap_size=2000000 \
              -Dswat.n_native_input_buffers=$NINPUTS \
              -Dswat.n_native_output_buffers=$NOUTPUTS \
              -Dswat.heaps_per_device=$HEAPS_PER_DEVICE"

spark-submit --class SparkConnectedComponents --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --master spark://localhost:7077 \
        $SCRIPT_DIR/target/sparkconnectedcomponents-0.0.0.jar \
        run $1 hdfs://$(hostname):54310/hyperlink-graph-links 39524212 2
