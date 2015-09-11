#!/bin/bash

set -e

NPROCS=12

for i in $(seq 0 $(echo $NPROCS - 1 | bc)); do
    /opt/apps/software/Core/Java/1.8.0_45/bin/java \
        -cp $SWAT_HOME/dataset-transformations/imagenet/target/imagenet-0.0.0.jar \
        FindSmallestImage $i $NPROCS $SPARK_DATASETS/imagenet/0.original &
done
wait
