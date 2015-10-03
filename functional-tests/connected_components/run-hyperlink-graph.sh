#!/bin/bash

set -e

if [[ $# != 1 ]]; then
    echo 'usage: run-hyperlink-graph.sh use-swat?'
    exit 1
fi

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep hyperlink-graph-links | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /hyperlink-graph-links
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/hyperlinkgraph/1.normalized/part* /hyperlink-graph-links
fi

# --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \

spark-submit --class SparkConnectedComponents --jars ${SWAT_JARS} \
        --conf "spark.executor.extraJavaOptions=-Dswat.input_chunking=100000 -Dswat.cl_local_size=128" \
        --master spark://localhost:7077 \
        $SCRIPT_DIR/target/sparkconnectedcomponents-0.0.0.jar \
        run $1 hdfs://$(hostname):54310/hyperlink-graph-links 39524212 2
