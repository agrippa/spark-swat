#!/bin/bash

if [[ $# != 1 && $# != 2 ]]; then
    echo "usage run.sh run|check use-swat?"
    exit 1
fi

spark-submit --class SparkSimple \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/object-input-object-output/target/sparksimple-0.0.0.jar \
        $1 $2 hdfs://$(hostname):54310/converted
