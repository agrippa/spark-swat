#!/bin/bash

set -e

if [[ $# != 1 ]]; then
    echo usage: distribute-kernel.sh kernel-file
    exit 1
fi

KERNEL_FILE=$1
FOUND=$(cat $KERNEL_FILE | grep "apply(This" | grep static | wc -l)
if [[ $FOUND != 1 ]]; then
    echo "Unable to find lambda class name in kernel file, FOUND=$FOUND. Is this a valid kernel file?"
    exit 1
fi

KERNEL_NAME=$(cat $KERNEL_FILE | grep "apply(This" | grep static | awk '{ print $4 }')
KERNEL_NAME=${KERNEL_NAME:1:${#KERNEL_NAME}-13}

DST=/tmp/$KERNEL_NAME.kernel
DST=$(echo $DST | sed -e 's/\$/\\\$/g')
NODES=$(scontrol show hostname)

for N in $NODES; do
    echo $KERNEL_FILE $N:$DST
    scp $KERNEL_FILE "$N:$DST"
done
