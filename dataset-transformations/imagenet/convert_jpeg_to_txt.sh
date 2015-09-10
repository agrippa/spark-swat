#!/bin/bash

set -e

NPROCS=12

for i in $(seq 0 $(echo $NPROCS - 1 | bc)); do
    java -cp $SWAT_HOME/dataset-transformations/imagenet/target/imagenet-0.0.0.jar \
             ConvertJPEGsToText $i $NPROCS $SPARK_DATASETS/imagenet/0.original \
             $SPARK_DATASETS/imagenet/1.txt
done
