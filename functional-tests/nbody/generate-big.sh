#!/bin/bash

# output-dir n-files points-per-file info-file pairs-output-dir pairs-per-file
DATA_DIR=$SPARK_DATA/nbody

rm -rf $DATA_DIR/*

mkdir $DATA_DIR/points
mkdir $DATA_DIR/pairs

scala -classpath ./target/sparknbody-0.0.0.jar GenerateInput \
          /scratch/jmg3/spark-inputs/nbody/points 100 200 \
          /scratch/jmg3/spark-inputs/nbody/info /scratch/jmg3/spark-inputs/nbody/pairs 4000000
