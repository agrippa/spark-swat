#!/bin/bash

set -e

${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /filtered-links
${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /renormalized-links

${HADOOP_HOME}/bin/hdfs dfs -mkdir /filtered-links
${HADOOP_HOME}/bin/hdfs dfs -put $SPARK_DATASETS/hyperlinkgraph/2.filter/part* /filtered-links/

spark-submit --class HyperlinkRenormalizer \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 --conf "spark.driver.maxResultSize=4g" \
        --conf "spark.storage.memoryFraction=0.3" \
        ${SWAT_HOME}/dataset-transformations/hyperlinkgraph/target/hyperlink-graph-0.0.0.jar \
        hdfs://$(hostname):54310/filtered-links hdfs://$(hostname):54310/renormalized-links

rm -rf $SPARK_DATASETS/hyperlinkgraph/3.renormalize
${HADOOP_HOME}/bin/hdfs dfs -get /renormalized-links $SPARK_DATASETS/hyperlinkgraph/3.renormalize
