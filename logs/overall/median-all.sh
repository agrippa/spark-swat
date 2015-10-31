for dir in $(ls); do
    if [[ $dir != median-all.sh && $dir != extract_stage_times.sh ]]; then
        SPARK_MEDIAN=$(cat $dir/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/median.py);
        SWAT_MEDIAN=$(cat $dir/swat |  grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/median.py);
        SPARK_MEAN=$(cat $dir/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        SWAT_MEAN=$(cat $dir/swat |  grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/mean.py);
        echo $dir $SPARK_MEDIAN $SWAT_MEDIAN $(echo $SPARK_MEDIAN / $SWAT_MEDIAN | bc -l) $(echo $SPARK_MEAN / $SWAT_MEAN | bc -l)
    fi
done
