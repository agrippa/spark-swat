#!/bin/bash

set -e

DATA_DIR=$SPARK_DATA/kmeans
rm -rf $DATA_DIR/*

# n-output-files npoints-per-file nclusters

for POINTS_PER_FILE in 50000 100000 150000; do
    for NCLUSTERS in 100 200 300; do
        OUTPUT_DIR=$DATA_DIR/${POINTS_PER_FILE}points_per_file-${NCLUSTERS}clusters
        mkdir -p $OUTPUT_DIR/points
        scala -classpath ./target/sparkkmeans-0.0.0.jar GenerateInput \
            $OUTPUT_DIR/points 100 100000 100 $OUTPUT_DIR/info
    done
done
