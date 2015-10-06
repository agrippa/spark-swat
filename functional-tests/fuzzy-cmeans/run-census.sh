#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $SCRIPT_DIR/../common.sh

if [[ $# != 3 ]]; then
    echo usage: run.sh niters ncenters use-swat?
    exit 1
fi

ITERS=$1
CENTERS=$2
USE_SWAT=$3

INPUT_EXISTS=$(${HADOOP_HOME}/bin/hdfs dfs -ls / | grep census-data | wc -l)
if [[ $INPUT_EXISTS != 1 ]]; then
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir /census-data
    ${HADOOP_HOME}/bin/hdfs dfs -put \
        $SPARK_DATASETS/census/3.dup/part* /census-data/
fi
# --conf "spark.executor.extraJavaOptions=-XX:GCTimeRatio=19 -Xloggc:/tmp/SWAT.log -verbose:gc" \

spark-submit --class SparkFuzzyCMeans --jars ${SWAT_JARS} \
        --conf "spark.executor.extraJavaOptions=-Dswat.cl_local_size=128" \
        --master spark://localhost:7077 ${SCRIPT_DIR}/target/sparkfuzzycmeans-0.0.0.jar \
        run $CENTERS $ITERS hdfs://$(hostname):54310/census-data $USE_SWAT
