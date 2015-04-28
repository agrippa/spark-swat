#!/bin/bash

for node in $(cat ${HADOOP_HOME}/etc/hadoop/slaves ${HADOOP_HOME}/etc/hadoop/masters); do
    PIDS=$(ssh $node "ps aux | grep \"hadoop\\|spark\" | grep -v grep | grep -v force_kill | awk '{ print \$2 }'")

    for pid in $PIDS; do
        ssh $node "kill -9 $pid"
    done

    ssh $node "rm -rf /tmp/hadoop-jmg3/dfs/data"

    echo Killed $(echo $PIDS | wc -w) processes in $node
done

rm -rf $SPARK_HOME/work/*
