#!/bin/bash


./test_translator.sh $* 2>&1 &> failed.log

set -e

FAILED_TEST=$(cat failed.log | grep FAILED | awk '{ print $1 }')

if [[ -z "$FAILED_TEST" ]]; then
    echo No failing tests
    exit 1
fi
FAILED_TEST=${FAILED_TEST%?}

REF_FILE=$(cat failed.log | grep '^Reference file' | awk '{ print $5 }')

echo "Updating $FAILED_TEST @ $REF_FILE"
mkdir -p $(dirname $REF_FILE)
cp generated $REF_FILE
