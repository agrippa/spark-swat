for dir in $(ls); do
    if [[ $dir != median-all.sh ]]; then
        SPARK=$(cat $dir/spark | grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/median.py);
        SWAT=$(cat $dir/swat |  grep "overall\|Overall" | grep time | \
                awk '{ print $4 }' | python ~/median.py);
        echo $dir $SPARK $SWAT $(echo $SPARK / $SWAT | bc -l);
    fi
done
