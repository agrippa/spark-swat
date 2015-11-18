#!/bin/bash

set -e

if [[ ! -z $SLURM_JOBID ]]; then
    scontrol show hostname | sort | uniq | grep -v $(hostname -s) > $SPARK_HOME/conf/slaves
    cp $SPARK_HOME/conf/slaves $HADOOP_HOME/etc/hadoop/slaves
    echo $(hostname -s) > $HADOOP_HOME/etc/hadoop/masters
fi
