#!/bin/bash

if [[ $# != 1 ]]; then
    echo 'usage: run.sh run|run-cl|check'
    exit 1
fi
# --conf "spark.executor.extraJavaOptions=-Xloggc:/tmp/SWAT.log -verbose:gc -verbose:jni -XX:+PrintGCDetails" \
# --conf "spark.executor.extraJavaOptions=-Xloggc:/tmp/SWAT.log -verbose:jni" \

spark-submit --class SparkConnectedComponents \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/connected_components/target/sparkconnectedcomponents-0.0.0.jar \
        $1 hdfs://$(hostname):54310/converted-links $SPARK_DATA/connected_components/info
