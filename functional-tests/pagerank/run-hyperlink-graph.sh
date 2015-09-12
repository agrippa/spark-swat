#!/bin/bash

set -e

if [[ $# != 2 ]]; then
    echo 'usage: run-hyperlink-graph.sh use-swat? iters'
    exit 1
fi

USE_SWAT=$1
ITERS=$2

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep hyperlink-graph-links | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /hyperlink-graph-links
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/1.normalized/part* /hyperlink-graph-links
fi

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep hyperlink-docs | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /hyperlink-docs
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/2.doc-ranks/part* /hyperlink-docs
fi

spark-submit --class SparkPageRank --jars ${SWAT_JARS} \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        $SCRIPT_DIR/target/sparkpagerank-0.0.0.jar \
        run $ITERS hdfs://$(hostname):54310/hyperlink-graph-links \
        hdfs://$(hostname):54310/hyperlink-docs $USE_SWAT
