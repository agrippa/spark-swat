#!/bin/bash

if [[ ! -f mnist.pkl.gz ]]; then
    wget http://deeplearning.net/data/mnist/mnist.pkl.gz
fi

# Change the argument after the .gz path to -1 to run with the whole training dataset
# path-to-mnist.pkl.gz limit-data per-file training-dir
# training-correct-dir testing-dir
# testing-correct-dir info-file nclasses layer1dim layer2dim ...
N_DATAPOINTS="-1"
PER_FILE=1000
NCLASSES=10
python generate.py mnist.pkl.gz $N_DATAPOINTS $PER_FILE \
           ${SPARK_DATA}/nn/training ${SPARK_DATA}/nn/correct \
           ${SPARK_DATA}/nn/testing ${SPARK_DATA}/nn/testing-correct \
           ${SPARK_DATA}/nn/info $NCLASSES 30
