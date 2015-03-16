#!/bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

JARS=${SWAT_HOME}/swat/target/swat-1.0-SNAPSHOT.jar,${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar,${ASM_HOME}/lib/asm-5.0.3.jar,${ASM_HOME}/lib/asm-util-5.0.3.jar,${SCALA_HOME}/lib/scala-library.jar
PWD=$(pwd)

force_kill.sh
teardown_all.sh

setup_all.sh

for f in $(ls ${SCRIPT_DIR}); do
    if [[ -d ${SCRIPT_DIR}/$f ]]; then
        TEST_NAME=$(basename $f)

        echo SWAT Cleaning up prior to $TEST_NAME
        # Clear out any top-level files/folders in HDFS
        for hdf in $(hdfs dfs -ls / | awk '{ print $8 }'); do
            hdfs dfs -rm -r $hdf
        done

        echo SWAT Initializing $TEST_NAME
        cd ${SCRIPT_DIR}/$f && ./init.sh && cd ${PWD}

        echo SWAT Running $TEST_NAME
        set +e
        cd $SCRIPT_DIR/$f && spark-submit --class SparkSimple --jars $JARS \
            --master spark://localhost:7077 target/sparksimple-0.0.0.jar \
                check hdfs://$(hostname):54310/converted &> test.log && cd $PWD
        ERR=$?
        set -e
        echo SWAT Test $TEST_NAME finished with error $ERR
        if [[ $ERR -gt 0 ]]; then
            echo SWAT Test $TEST_NAME failed
            if [[ -f ${SCRIPT_DIR}/${f}/test.log ]]; then
                cat ${SCRIPT_DIR}/${f}/test.log
            fi
            exit 1
        fi
    fi
done
