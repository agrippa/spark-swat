#!/bin/bash

set -e

while :; do
    ./update_last_failed_test.sh
    ERR=$?
    if [[ $ERR == 1 ]]; then
        break
    fi
done
