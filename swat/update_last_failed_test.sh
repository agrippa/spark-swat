#!/bin/bash

set -e

FAILED_TEST=$(./test_translator.sh $* 2>&1 | grep FAILED | awk '{ print $1 }')
if [[ -z "$FAILED_TEST" ]]; then
    echo No failing tests
    exit 1
fi
FAILED_TEST=${FAILED_TEST%?}

echo Updating $FAILED_TEST
./update_test.sh $FAILED_TEST
