#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-correct-input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /testing-input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /testing-correct-input

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-converted
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /training-correct-converted
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /testing-converted
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /testing-correct-converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /training-input
${HADOOP_HOME}/bin/hdfs dfs -mkdir /training-correct-input
${HADOOP_HOME}/bin/hdfs dfs -mkdir /testing-input
${HADOOP_HOME}/bin/hdfs dfs -mkdir /testing-correct-input

${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/training/input.* /training-input/
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/correct/input.* /training-correct-input/
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/testing/input.* /testing-input/
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATA/nn/testing-correct/input.* /testing-correct-input/

spark-submit --class SparkNN --master spark://localhost:7077 \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        $SCRIPT_DIR/target/nn-0.0.0.jar convert \
        hdfs://$(hostname):54310/training-input \
        hdfs://$(hostname):54310/training-converted \
        hdfs://$(hostname):54310/training-correct-input \
        hdfs://$(hostname):54310/training-correct-converted \
        hdfs://$(hostname):54310/testing-input \
        hdfs://$(hostname):54310/testing-converted \
        hdfs://$(hostname):54310/testing-correct-input \
        hdfs://$(hostname):54310/testing-correct-converted

