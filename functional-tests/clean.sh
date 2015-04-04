#!/bin/bash

for f in $(hdfs dfs -ls / | grep $USER | awk '{ print $8 }'); do
    hdfs dfs -rm -r $f
done
