#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 1 && $# != 2 ]]; then
    echo 'usage: run.sh check'
    echo '       run.sh run use-swat?'
    exit 1
fi

spark-submit --class SparkNN --jars ${SWAT_JARS} \
        --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19" \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/nn/target/nn-0.0.0.jar \
        $1 $2 $SPARK_DATA/nn/info \
        hdfs://$(hostname):54310/training-converted \
        hdfs://$(hostname):54310/training-correct-converted \
        1 3.0
