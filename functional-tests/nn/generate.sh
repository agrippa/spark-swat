#!/bin/bash

set -e

if [[ ! -f mnist.pkl.gz ]]; then
    wget http://deeplearning.net/data/mnist/mnist.pkl.gz
fi

mkdir -p ${SPARK_DATA}/nn/training
mkdir -p ${SPARK_DATA}/nn/correct
mkdir -p ${SPARK_DATA}/nn/testing
mkdir -p ${SPARK_DATA}/nn/testing-correct

# Change the argument after the .gz path to -1 to run with the whole training dataset
# path-to-mnist.pkl.gz limit-data per-file training-dir
# training-correct-dir testing-dir
# testing-correct-dir info-file nclasses layer1dim layer2dim ...
N_DATAPOINTS="-1"
PER_FILE=10000
NCLASSES=10
TARGET_N_TRAINING_FILES=24
python generate.py mnist.pkl.gz $N_DATAPOINTS $PER_FILE \
           ${SPARK_DATA}/nn/training ${SPARK_DATA}/nn/correct \
           ${SPARK_DATA}/nn/testing ${SPARK_DATA}/nn/testing-correct \
           ${SPARK_DATA}/nn/info $NCLASSES $TARGET_N_TRAINING_FILES 768
