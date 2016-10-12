#!/bin/bash

for node in $(cat ${HADOOP_HOME}/etc/hadoop/slaves ${HADOOP_HOME}/etc/hadoop/masters); do
    PIDS=$(ssh $node "ps aux | grep \"hadoop\\|spark\" | grep -v grep | grep -v force_kill | grep $(whoami) | awk '{ print \$2 }'")

    for pid in $PIDS; do
        ssh $node "kill -9 $pid"
    done

    DATA_DIR=/tmp/hadoop-$(whoami)/dfs/data
    echo Clearing out directory $DATA_DIR on node $node
    ssh $node "rm -rf $DATA_DIR"

    echo Killed $(echo $PIDS | wc -w) processes in $node
done

rm -rf $SPARK_HOME/work/*
rm -rf $HADOOP_HOME/logs/*
