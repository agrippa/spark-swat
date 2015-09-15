#!/bin/bash

set -e

if [[ $# != 1 ]]; then
    echo usage: 2.dup.sh n-duplicates
    exit 1
fi

TARGETS=$(ls $SPARK_DATASETS/census/2.split | grep part)
rm -rf $SPARK_DATASETS/census/3.dup
mkdir -p $SPARK_DATASETS/census/3.dup

for i in $(seq 1 $1); do
    for TARGET in $TARGETS; do
        export SRC=$SPARK_DATASETS/census/2.split/$TARGET
        export DST=$SPARK_DATASETS/census/3.dup/$TARGET.$i
        echo "$SRC => $DST"
        cp $SRC $DST
    done
done

echo Created $1 copies
