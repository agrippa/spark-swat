#!/bin/bash

# usage: GenerateInput output-links-dir n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file
DATA_DIR=$SPARK_DATA/connected_components
mkdir -p $DATA_DIR
mkdir -p $DATA_DIR/links
rm -f $DATA_DIR/links/*

# scala -classpath ./target/sparkconnectedcomponents-0.0.0.jar GenerateInput \
#           $DATA_DIR/links 1000 10000000 300000000 $DATA_DIR/info
scala -classpath ./target/sparkconnectedcomponents-0.0.0.jar GenerateInput \
          $DATA_DIR/links 500 5000000 70000000 $DATA_DIR/info
