#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f /input-docs
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-docs

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
${HADOOP_HOME}/bin/hdfs dfs -put /scratch/jmg3/spark-inputs/pagerank/links/input.* /input-links/
${HADOOP_HOME}/bin/hdfs dfs -put /scratch/jmg3/spark-inputs/pagerank/docs /input-docs

spark-submit --class SparkPageRank --master spark://localhost:7077 \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        ${SWAT_HOME}/functional-tests/pagerank/target/sparkpagerank-0.0.0.jar convert \
        hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/converted-links \
        hdfs://$(hostname):54310/input-docs hdfs://$(hostname):54310/converted-docs
