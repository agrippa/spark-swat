#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/connected_components/links/input.* /input-links/

spark-submit --class SparkConnectedComponents --master spark://localhost:7077 \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        ${SWAT_HOME}/functional-tests/connected_components/target/sparkconnectedcomponents-0.0.0.jar convert \
        hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/converted-links
