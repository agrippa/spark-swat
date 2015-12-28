#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 1 && $# != 2 ]]; then
    echo "usage run.sh run|check use-swat"
    exit 1
fi

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.print_kernel=true"

spark-submit --class SparkSimple \
        --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/map-async-metadata/target/sparksimple-0.0.0.jar \
        $1 $2 hdfs://$(hostname):54310/converted
