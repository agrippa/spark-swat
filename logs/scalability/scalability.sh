#!/bin/bash

set -e

for dir in $(ls); do
    if [[ $dir != median-all.sh && $dir != extract_stage_times.sh && $dir != scalability.sh ]]; then
        SPARK_2_MEAN=$(cat $dir/2/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        SPARK_4_MEAN=$(cat $dir/4/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        SPARK_8_MEAN=$(cat $dir/8/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);

        SWAT_2_MEAN=$(cat $dir/2/swat | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        SWAT_4_MEAN=$(cat $dir/4/swat | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        SWAT_8_MEAN=$(cat $dir/8/swat | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);

        echo $dir spark $(echo $SPARK_2_MEAN / $SPARK_4_MEAN | bc -l) \
            $(echo $SPARK_4_MEAN / $SPARK_8_MEAN | bc -l)
        echo $dir swat $(echo $SWAT_2_MEAN / $SWAT_4_MEAN | bc -l) \
            $(echo $SWAT_4_MEAN / $SWAT_8_MEAN | bc -l)
    fi
done
