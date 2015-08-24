#!/bin/bash

if [[ $# != 2 ]]; then
    echo usage: run.sh niters use-swat?
    exit 1
fi

# --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:-ResizePLAB -Xloggc:/tmp/SWAT.log -verbose:gc -verbose:jni -XX:+PrintGCDetails" \
# --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:-ResizePLAB -XX:ConcGCThreads=8" \
# --conf "spark.executor.extraJavaOptions=-Xloggc:/tmp/SWAT.log -verbose:gc -verbose:jni -XX:+PrintGCDetails" \

spark-submit --class SparkFuzzyCMeans \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/fuzzy-cmeans/target/sparkfuzzycmeans-0.0.0.jar \
        run 160 $1 hdfs://$(hostname):54310/converted $2
