#!/bin/bash

# output-dir n-files points-per-file info-file pairs-output-dir pairs-per-file
DATA_DIR=$SPARK_DATA/nbody

rm -rf $DATA_DIR/*

for POINTS_PER_FILE in 200 400 800; do
    OUTPUT_DIR=$DATA_DIR/${POINTS_PER_FILE}points_per_file
    mkdir -p $OUTPUT_DIR/points
    mkdir -p $OUTPUT_DIR/pairs

    scala -classpath ./target/sparknbody-0.0.0.jar GenerateInput \
              $OUTPUT_DIR/points 100 $POINTS_PER_FILE $OUTPUT_DIR/info \
              $OUTPUT_DIR/pairs 4000000
done
