#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 1 && $# != 2 ]]; then
    echo "usage run.sh run|check use-swat"
    exit 1
fi

CMD=$1
USE_SWAT=$2

# SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.print_kernel=true -Dswat.kernels_dir=/home/jmg3/spark-swat/functional-tests/tuple-dense-input/kernels"
SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.print_kernel=true"


spark-submit --class SparkSimple --jars ${SWAT_JARS} --conf "$SWAT_OPTIONS" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/tuple-dense-input/target/sparksimple-0.0.0.jar \
        $CMD $USE_SWAT hdfs://$(hostname):54310/converted
