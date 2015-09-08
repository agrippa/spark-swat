#!/bin/bash

CMD="$*"

ERR=0

while [[ $ERR == 0 ]]; do
    $(eval $CMD)
    ERR=$?
    echo ERR=$ERR
done
