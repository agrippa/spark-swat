#!/bin/bash

if [[ $# != 1 && $# != 2 ]]; then
    echo "usage run.sh cmd [use-swat?]"
    echo "  where cmd is run or check"
    echo "  if cmd is run, the next argument must be use-swat?. otherwise it must be empty"
    exit 1
fi
CMD=$1

if [[ $CMD != "run" && $CMD != "check" ]]; then
    echo "Invalid command"
    exit 1
fi

spark-submit --class SparkSimple \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/broadcast-dense/target/broadcast_dense-0.0.0.jar \
        $CMD $2 hdfs://$(hostname):54310/converted
