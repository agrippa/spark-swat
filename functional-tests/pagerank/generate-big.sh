#!/bin/bash

set -e

DATA_DIR=$SPARK_DATA/pagerank
rm -rf $DATA_DIR/*

# usage: GenerateInput output-links-dir n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file
for NDOCS in 100000 500000 1000000; do
    for AVG_N_LINKS in 50 100 150; do
        echo $NDOCS docs, $AVG_N_LINKS average links
        OUTPUT_DIR=$DATA_DIR/${NDOCS}docs-${AVG_N_LINKS}links
        mkdir -p $OUTPUT_DIR/links
        scala -classpath ./target/sparkpagerank-0.0.0.jar GenerateInput \
            ${OUTPUT_DIR}/links 100 ${NDOCS} ${AVG_N_LINKS} 10 ${OUTPUT_DIR}/docs
    done
done
