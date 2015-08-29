#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /correct-input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-converted
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /correct-converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /training-input
${HADOOP_HOME}/bin/hdfs dfs -mkdir /correct-input
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/training/input.* /training-input/
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/correct/input.* /correct-input/

spark-submit --class SparkNN --master spark://localhost:7077 \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        $SCRIPT_DIR/target/nn-0.0.0.jar convert \
        hdfs://$(hostname):54310/training-input hdfs://$(hostname):54310/training-converted \
        hdfs://$(hostname):54310/correct-input hdfs://$(hostname):54310/correct-converted
