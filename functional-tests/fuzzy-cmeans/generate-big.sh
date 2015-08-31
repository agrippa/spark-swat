#!/bin/bash

set -e

DATA_DIR=$SPARK_DATA/fuzzycmeans
rm -rf $DATA_DIR/*

# output-dir n-files points-per-file nclusters

for POINTS_PER_FILE in 36000 48000 64000; do
    for NCLUSTERS in 80 160 320; do
        OUTPUT_DIR=$DATA_DIR/${POINTS_PER_FILE}points_per_file-${NCLUSTERS}clusters
        mkdir -p $OUTPUT_DIR/points
        scala -classpath ./target/sparkfuzzycmeans-0.0.0.jar GenerateInput \
            $OUTPUT_DIR/points 375 $POINTS_PER_FILE $NCLUSTERS $OUTPUT_DIR/info
    done
done
