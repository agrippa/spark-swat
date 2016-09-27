#!/bin/bash

mkdir -p $SPARK_DATASETS/imagenet/2.merged
rm -f $SPARK_DATASETS/imagenet/2.merged/*

for i in {0..5}; do
    echo $i
    for f in $(ls $SPARK_DATASETS/imagenet/1.txt/ILSVRC2012_test_000${i}*); do
        cat $f; echo;
    done > $SPARK_DATASETS/imagenet/2.merged/COMBINED.${i} &
done
wait
