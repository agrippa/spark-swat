#!/bin/bash

set -e

NODES=$(scontrol show hostname)
for N in $NODES; do
    echo $N
    ssh $N 'rm -v /tmp/*.kernel'
done
