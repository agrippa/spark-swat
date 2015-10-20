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

SWAT_OPTIONS="spark.executor.extraJavaOptions=-Dswat.cl_local_size=256 \
              -Dswat.input_chunking=100 -Dswat.heap_size=67108864 \
              -Dswat.n_native_input_buffers=3 \
              -Dswat.n_native_output_buffers=3 \
              -Dswat.heaps_per_device=3 -Dswat.print_kernel=false -Dswat.avg_vec_length=1"


spark-submit --class SparkSimple --jars ${SWAT_JARS} \
        --master spark://localhost:7077 \
        ${SWAT_HOME}/functional-tests/tuple-dense-input/target/sparksimple-0.0.0.jar \
        $CMD $USE_SWAT hdfs://$(hostname):54310/converted
