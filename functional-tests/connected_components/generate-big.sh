#!/bin/bash

DATA_DIR=$SPARK_DATA/connected_components
rm -rf $DATA_DIR/*

for NODES in 3000000 5000000 10000000; do
    for LINKS in 30000000 50000000 70000000; do
        OUTPUT_FOLDER=$DATA_DIR/${NODES}nodes-${LINKS}links
        # link-dir output-link-files n-nodes n-links info-file
        mkdir -p $OUTPUT_FOLDER/links
        scala -classpath ./target/sparkconnectedcomponents-0.0.0.jar GenerateInput \
                  $OUTPUT_FOLDER/links 50 5000000 70000000 $OUTPUT_FOLDER/info
    done
done

