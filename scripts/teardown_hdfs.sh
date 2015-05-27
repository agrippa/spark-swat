#!/bin/bash

set -e

${HADOOP_HOME}/sbin/stop-dfs.sh
# Delete on the master node
echo 'Clearing out HDFS on the Namenode (localhost)'
rm -rf /tmp/hadoop-$(whoami)/dfs
for node in $(cat $HADOOP_HOME/etc/hadoop/slaves); do
    echo "Clearing out HDFS on Datanode $node"
    ssh $node "rm -rf /tmp/hadoop-$(whoami)/dfs"
done

rm -rf $HADOOP_HOME/logs/*
