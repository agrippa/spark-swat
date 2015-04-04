#!/bin/bash

# Generate input if none exists
mkdir -p input
EXISTING_INPUTS=$(ls -la input/ | wc -l)
if [[ $EXISTING_INPUTS != 14 ]]; then
    rm -rf input/*
    ./generate.sh
fi

# Place input in HDFS
./convert.sh
