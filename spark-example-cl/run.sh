#!/bin/bash

if [[ $# != 1 ]]; then
    echo usage: run.sh niters
    exit 1
fi

spark-submit --class SparkKMeans \
        --jars ${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/spark-example-cl/target/sparkkmeans-0.0.0.jar \
        run 3 $1 hdfs://$(hostname):54310/converted
