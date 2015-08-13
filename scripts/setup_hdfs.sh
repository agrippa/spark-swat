#!/bin/bash

set -e

rm -rf /tmp/hadoop-$(whoami)/dfs
${HADOOP_HOME}/sbin/stop-dfs.sh
rm -rf /tmp/hadoop-$(whoami)/dfs

echo Sedding core-site
sed  "s/MASTER/$(hostname)/g" $HADOOP_HOME/etc/hadoop/core-site.xml.template > \
         $HADOOP_HOME/etc/hadoop/core-site.xml
echo Updating hdfs-site
cp $HADOOP_HOME/etc/hadoop/hdfs-site.xml.template $HADOOP_HOME/etc/hadoop/hdfs-site.xml

${HADOOP_HOME}/bin/hdfs namenode -format
${HADOOP_HOME}/sbin/start-dfs.sh
