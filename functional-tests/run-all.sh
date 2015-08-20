#!/bin/bash

set -e

OUTPUT=perf.log

# connected_components
cd connected_components
./convert-big.sh
./run-big.sh run &> spark.log
./run-big.sh run-cl &> swat.log
CC_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
CC_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
CC_SPEEDUP=$(echo $CC_SPARK_TIME / $CC_SWAT_TIME | bc -l)
cd ..

# fuzzy cmeans
cd fuzzy-cmeans
./convert-big.sh
./run-big.sh 3 false &> spark.log
./run-big.sh 3 true &> swat.log
FUZZY_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
FUZZY_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
FUZZY_SPEEDUP=$(echo $FUZZY_SPARK_TIME / $FUZZY_SWAT_TIME | bc -l)
cd ..

# kmeans
cd kmeans
./convert-big.sh
./run-big.sh 3 false &> spark.log
./run-big.sh 3 true &> swat.log
KMEANS_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
KMEANS_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
KMEANS_SPEEDUP=$(echo $KMEANS_SPARK_TIME / $KMEANS_SWAT_TIME | bc -l)
cd ..

# pagerank
cd pagerank
./convert-big.sh
./run-big.sh 3 false &> spark.log
./run-big.sh 3 true &> swat.log
PAGERANK_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
PAGERANK_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
PAGERANK_SPEEDUP=$(echo $PAGERANK_SPARK_TIME / $PAGERANK_SWAT_TIME | bc -l)
cd ..

echo ===== Connected Components ===== > $OUTPUT
echo Spark : $CC_SPARK_TIME >> $OUTPUT
echo SWAT : $CC_SWAT_TIME >> $OUTPUT
echo Speedup : $CC_SPEEDUP >> $OUTPUT
echo >> $OUTPUT
echo ===== Fuzzy CMeans ===== >> $OUTPUT
echo Spark : $FUZZY_SPARK_TIME >> $OUTPUT
echo SWAT : $FUZZY_SWAT_TIME >> $OUTPUT
echo Speedup : $FUZZY_SPEEDUP >> $OUTPUT
echo >> $OUTPUT
echo ===== KMeans ===== >> $OUTPUT
echo Spark : $KMEANS_SPARK_TIME >> $OUTPUT
echo SWAT : $KMEANS_SWAT_TIME >> $OUTPUT
echo Speedup : $KMEANS_SPEEDUP >> $OUTPUT
echo >> $OUTPUT
echo ===== PageRank ===== >> $OUTPUT
echo Spark : $PAGERANK_SPARK_TIME >> $OUTPUT
echo SWAT : $PAGERANK_SWAT_TIME >> $OUTPUT
echo Speedup : $PAGERANK_SPEEDUP >> $OUTPUT
echo >> $OUTPUT

if [[ -f $OUTPUT.old ]]; then
    cat $OUTPUT
    echo
    echo Past results:
    cat $OUTPUT.old
fi
