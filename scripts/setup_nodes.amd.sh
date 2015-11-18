#!/bin/bash

set -e

NAMENODE=""

truncate -s 0 $SPARK_HOME/conf/slaves
for NODE in $*; do
    if [[ -z $NAMENODE ]]; then
        NAMENODE=$NODE
        echo $NAMENODE > $HADOOP_HOME/etc/hadoop/masters
    else
        echo $NODE >> $SPARK_HOME/conf/slaves
    fi
done

cp $SPARK_HOME/conf/slaves $HADOOP_HOME/etc/hadoop/slaves

if [[ -z $NAMENODE ]]; then
    echo "No nodes specified"
    exit 1
fi

echo Namenode = $NAMENODE
