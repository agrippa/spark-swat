#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
${HADOOP_HOME}/bin/hdfs dfs -put /scratch/jmg3/spark-inputs/nbody/points/input.* /input/

spark-submit --class SparkNBody --master spark://localhost:7077 \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        ${SWAT_HOME}/functional-tests/nbody/target/sparknbody-0.0.0.jar convert \
        hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted
