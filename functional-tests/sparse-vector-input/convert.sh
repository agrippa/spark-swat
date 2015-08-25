#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
${HADOOP_HOME}/bin/hdfs dfs -put $SCRIPT_DIR/input/input.* /input/

spark-submit --class SparkSimple --master spark://localhost:7077 \
        $SCRIPT_DIR/target/sparse_vector_input-0.0.0.jar convert \
        hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted
