#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f /input-docs
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-docs

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
${HADOOP_HOME}/bin/hdfs dfs -put ${SWAT_HOME}/functional-tests/pagerank/input/links/input.* /input-links/
${HADOOP_HOME}/bin/hdfs dfs -put ${SWAT_HOME}/functional-tests/pagerank/input/docs /input-docs

spark-submit --class SparkPageRank --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/pagerank/target/sparkpagerank-0.0.0.jar convert \
        hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/converted-links \
        hdfs://$(hostname):54310/input-docs hdfs://$(hostname):54310/converted-docs
