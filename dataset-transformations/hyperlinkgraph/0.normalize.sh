#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /normalized-links

${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATASETS/hyperlinkgraph/0.original/part* /input-links/

spark-submit --class HyperlinkGraphNormalizer \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 --conf "spark.driver.maxResultSize=4g" \
        --conf "spark.storage.memoryFraction=0.3" \
        ${SWAT_HOME}/dataset-transformations/hyperlinkgraph/target/hyperlink-graph-0.0.0.jar \
        hdfs://$(hostname):54310/input-links hdfs://$(hostname):54310/normalized-links

rm -rf $SPARK_DATASETS/hyperlinkgraph/1.normalized
${HADOOP_HOME}/bin/hdfs dfs -get /normalized-links $SPARK_DATASETS/hyperlinkgraph/1.normalized
