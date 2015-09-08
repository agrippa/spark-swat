#!/bin/bash

set -e

for t in $(cat tests); do
    echo $t

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
done
