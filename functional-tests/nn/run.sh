#!/bin/bash

if [[ $# != 1 && $# != 2 ]]; then
    echo 'usage: run.sh check'
    echo '       run.sh run use-swat?'
    exit 1
fi

spark-submit --class SparkNN \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        $1 $2 $SPARK_DATA/nn/info \
        hdfs://$(hostname):54310/training-converted \
        hdfs://$(hostname):54310/training-correct-converted \
        hdfs://$(hostname):54310/testing-converted \
        hdfs://$(hostname):54310/testing-correct-converted \
        1 3.0
