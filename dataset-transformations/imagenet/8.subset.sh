#!/bin/bash

mkdir -p $SPARK_DATASETS/imagenet/8.subset
for F in $(ls -l $SPARK_DATASETS/imagenet/7.multiply_again | grep 'part-' | awk '{ print $9 }' | tail -n 10); do
    cp $SPARK_DATASETS/imagenet/7.multiply_again/$F $SPARK_DATASETS/imagenet/8.subset/$F
done
