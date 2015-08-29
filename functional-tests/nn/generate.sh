#!/bin/bash

if [[ ! -f mnist.pkl.gz ]]; then
    wget http://deeplearning.net/data/mnist/mnist.pkl.gz
fi

python generate.py mnist.pkl.gz 1000 ${SPARK_DATA}/nn/training \
           ${SPARK_DATA}/nn/correct ${SPARK_DATA}/nn/info 10 1 1000
