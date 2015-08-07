#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/connected_components/links/input.* /input-links/

spark-submit --class SparkConnectedComponents --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/connected_components/target/sparkconnectedcomponents-0.0.0.jar convert \
        hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/converted-links
