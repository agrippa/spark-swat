#!/bin/bash

DATA_DIR=$SPARK_DATA/fuzzycmeans
mkdir -p $DATA_DIR
rm -f $DATA_DIR/*

# scala -classpath ./target/sparkfuzzycmeans-0.0.0.jar GenerateInput $DATA_DIR 750 48000 80
scala -classpath ./target/sparkfuzzycmeans-0.0.0.jar GenerateInput $DATA_DIR 375 48000 80
