#!/bin/bash

set -e

TESTNAME=
CONTINUE_AFTER=0
ENABLED=1

if [[ $# == 1 ]]; then
    TESTNAME=$1
    LAST_INDEX=$((${#TESTNAME}-1))
    LAST_CHAR="${TESTNAME:$LAST_INDEX:1}"
    if [[ $LAST_CHAR == "-" ]]; then
        TESTNAME="${TESTNAME:0:$LAST_INDEX}"
        CONTINUE_AFTER=1
    fi
    ENABLED=0
fi

echo "TESTNAME=${TESTNAME} CONTINUE_AFTER=${CONTINUE_AFTER}"
echo

for t in $(cat tests); do
    echo $t
    if [[ ! -z $TESTNAME && $TESTNAME == $t ]]; then
        ENABLED=1
    fi

    if [[ $ENABLED == 0 ]]; then
        continue
    fi

    cd $t
    mvn clean &> ../clean.log
    mvn package &> ../package.log
    ./generate.sh &> ../generate.log
    ./convert.sh &> ../convert.log
    ./run.sh check &> ../check.log
    cd ..

    set +e 
    cat check.log | grep PASSED
    ERR=$?
    set -e

    if [[ $ERR != 0 ]]; then
        echo FAILED
        exit 1
    fi

    if [[ ! -z $TESTNAME && $ENABLED == 1 && $CONTINUE_AFTER == 0 ]]; then
        ENABLED=0
    fi
done
