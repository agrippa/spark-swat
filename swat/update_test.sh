#!/bin/bash

if [[ $# != 1 ]]; then
    echo usage: update_test.sh test-name
    exit 1
fi

HOSTNAME=$(hostname -d 2> /dev/null)
ERR=$?
if [[ $ERR != 0 || -z $HOSTNAME ]]; then
    HOSTNAME=$(hostname)
fi
cp generated src/test/scala/org/apache/spark/rdd/cl/tests/$HOSTNAME/$1.kernel
