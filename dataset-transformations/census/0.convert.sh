#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATASETS/census/0.original/* /input/

spark-submit --class CensusConverter \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 --conf "spark.driver.maxResultSize=4g" \
        ${SWAT_HOME}/dataset-transformations/census/target/census-0.0.0.jar \
        hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted

rm -rf $SPARK_DATASETS/census/1.converted
${HADOOP_HOME}/bin/hdfs dfs -get /converted $SPARK_DATASETS/census/1.converted
