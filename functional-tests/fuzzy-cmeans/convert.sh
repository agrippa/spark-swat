#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
${HADOOP_HOME}/bin/hdfs dfs -put ${SWAT_HOME}/functional-tests/fuzzy-cmeans/input/input.* /input/

spark-submit --class SparkFuzzyCMeans --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/fuzzy-cmeans/target/sparkfuzzycmeans-0.0.0.jar convert \
        hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted
