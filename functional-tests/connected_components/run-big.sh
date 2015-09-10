#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 2 ]]; then
    echo 'usage: run.sh run|check use-swat?'
    exit 1
fi
# --conf "spark.executor.extraJavaOptions=-Xloggc:/tmp/SWAT.log -verbose:gc -verbose:jni -XX:+PrintGCDetails" \
# --conf "spark.executor.extraJavaOptions=-Xloggc:/tmp/SWAT.log -verbose:jni" \

# DATA_DIR=$SPARK_DATA/connected_components
# 
# for TEST in $(ls $DATA_DIR/); do
# 
#     echo $TEST
# 
#     ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /input-links
#     ${HADOOP_HOME}/bin/hdfs dfs -rm -f -r /converted-links
# 
#     ${HADOOP_HOME}/bin/hdfs dfs -mkdir /input-links
#     ${HADOOP_HOME}/bin/hdfs dfs -put $DATA_DIR/$TEST/links/input.* /input-links/
# 
#     spark-submit --class SparkConnectedComponents --master spark://localhost:7077 \
#             --jars ${SWAT_JARS} \
#             $SCRIPT_DIR/target/sparkconnectedcomponents-0.0.0.jar convert \
#             hdfs://$(hostname):54310/input-links \
#             hdfs://$(hostname):54310/converted-links
# 
#     spark-submit --class SparkConnectedComponents --jars ${SWAT_JARS} \
#             --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
#             --master spark://localhost:7077 \
#             $SCRIPT_DIR/target/sparkconnectedcomponents-0.0.0.jar \
#             $1 $2 hdfs://$(hostname):54310/converted-links
# done

spark-submit --class SparkConnectedComponents --jars ${SWAT_JARS} \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        $SCRIPT_DIR/target/sparkconnectedcomponents-0.0.0.jar \
        $1 $2 hdfs://$(hostname):54310/converted-links
