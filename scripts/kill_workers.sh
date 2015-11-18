#!/bin/bash

for node in $(cat ${HADOOP_HOME}/etc/hadoop/slaves ${HADOOP_HOME}/etc/hadoop/masters); do
    PIDS=$(ssh $node /opt/apps/software/Core/Java/1.7.0_80/bin/jps | grep CoarseGrainedExecutorBackend | awk '{ print $1 }')

    for pid in $PIDS; do
        ssh $node "kill -9 $pid"
    done
    echo Killed $(echo $PIDS | wc -w) processes in $node
done
