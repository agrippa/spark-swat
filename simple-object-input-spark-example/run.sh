#!/bin/bash

if [[ $# != 1 ]]; then
    echo "usage run.sh cmd"
    echo "  where cmd is run, run-cl, or check"
    exit 1
fi
CMD=$1

if [[ $CMD != "run" && $CMD != "run-cl" && $CMD != "check" ]]; then
    echo "Invalid command"
    exit 1
fi

spark-submit --class SparkSimple \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/simple-spark-example/target/sparksimple-0.0.0.jar \
        $CMD hdfs://$(hostname):54310/converted
