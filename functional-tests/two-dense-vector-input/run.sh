#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

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

spark-submit --class SparkSimple --jars ${SWAT_JARS} --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/two-dense-vector-input/target/dense_vector_input-0.0.0.jar \
        $CMD $2 hdfs://$(hostname):54310/converted
