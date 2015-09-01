#!/bin/bash

if [[ $# != 1 ]]; then
    echo usage: update_test.sh test-name
    exit 1
fi

cp generated src/test/scala/org/apache/spark/rdd/cl/tests/$(hostname)/$1.kernel
