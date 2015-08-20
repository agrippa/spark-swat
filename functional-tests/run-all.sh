#!/bin/bash

set -e

OUTPUT=perf.log
echo "" > $OUTPUT # clear it

TESTNAME=

while getopts "t:h" opt; do
    case $opt in
        t)
            TESTNAME=$OPTARG
            ;;
        h)
            echo 'usage: run-all.sh [-t test]'
            echo '    where test can be:'
            echo '        connected_components'
            echo '        fuzzy'
            echo '        kmeans'
            echo '        pagerank'
            echo '        nbody'
            exit 1
            ;;
        \?)
            echo "unrecognized option -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done

if [[ -z $TESTNAME || $TESTNAME == "connected_components" ]]; then
    # connected_components
    cd connected_components
    ./convert-big.sh
    ./run-big.sh run &> spark.log
    ./run-big.sh run-cl &> swat.log
    CC_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
    CC_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
    CC_SPEEDUP=$(echo $CC_SPARK_TIME / $CC_SWAT_TIME | bc -l)
    cd ..
    echo ===== Connected Components ===== >> $OUTPUT
    echo Spark : $CC_SPARK_TIME >> $OUTPUT
    echo SWAT : $CC_SWAT_TIME >> $OUTPUT
    echo Speedup : $CC_SPEEDUP >> $OUTPUT
    echo >> $OUTPUT
fi

if [[ -z $TESTNAME || $TESTNAME == "fuzzy" ]]; then
    # fuzzy cmeans
    cd fuzzy-cmeans
    ./convert-big.sh
    ./run-big.sh 3 false &> spark.log
    ./run-big.sh 3 true &> swat.log
    FUZZY_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
    FUZZY_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
    FUZZY_SPEEDUP=$(echo $FUZZY_SPARK_TIME / $FUZZY_SWAT_TIME | bc -l)
    cd ..
    echo ===== Fuzzy CMeans ===== >> $OUTPUT
    echo Spark : $FUZZY_SPARK_TIME >> $OUTPUT
    echo SWAT : $FUZZY_SWAT_TIME >> $OUTPUT
    echo Speedup : $FUZZY_SPEEDUP >> $OUTPUT
    echo >> $OUTPUT
fi

if [[ -z $TESTNAME || $TESTNAME == "kmeans" ]]; then
    # kmeans
    cd kmeans
    ./convert-big.sh
    ./run-big.sh 3 false &> spark.log
    ./run-big.sh 3 true &> swat.log
    KMEANS_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
    KMEANS_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
    KMEANS_SPEEDUP=$(echo $KMEANS_SPARK_TIME / $KMEANS_SWAT_TIME | bc -l)
    cd ..
    echo ===== KMeans ===== >> $OUTPUT
    echo Spark : $KMEANS_SPARK_TIME >> $OUTPUT
    echo SWAT : $KMEANS_SWAT_TIME >> $OUTPUT
    echo Speedup : $KMEANS_SPEEDUP >> $OUTPUT
    echo >> $OUTPUT
fi

if [[ -z $TESTNAME || $TESTNAME == "pagerank" ]]; then
    # pagerank
    cd pagerank
    ./convert-big.sh
    ./run-big.sh 3 false &> spark.log
    ./run-big.sh 3 true &> swat.log
    PAGERANK_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
    PAGERANK_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
    PAGERANK_SPEEDUP=$(echo $PAGERANK_SPARK_TIME / $PAGERANK_SWAT_TIME | bc -l)
    cd ..
    echo ===== PageRank ===== >> $OUTPUT
    echo Spark : $PAGERANK_SPARK_TIME >> $OUTPUT
    echo SWAT : $PAGERANK_SWAT_TIME >> $OUTPUT
    echo Speedup : $PAGERANK_SPEEDUP >> $OUTPUT
    echo >> $OUTPUT
fi

if [[ -z $TESTNAME || $TESTNAME == "nbody" ]]; then
    # nbody
    cd nbody
    ./convert-big.sh
    ./run-big.sh 3 false &> spark.log
    ./run-big.sh 3 true &> swat.log
    NBODY_SPARK_TIME=$(cat spark.log | grep "Overall time" | awk '{ print $4 }')
    NBODY_SWAT_TIME=$(cat swat.log | grep "Overall time" | awk '{ print $4 }')
    NBODY_SPEEDUP=$(echo $NBODY_SPARK_TIME / $NBODY_SWAT_TIME | bc -l)
    cd ..
    echo ===== NBody ===== >> $OUTPUT
    echo Spark : $NBODY_SPARK_TIME >> $OUTPUT
    echo SWAT : $NBODY_SWAT_TIME >> $OUTPUT
    echo Speedup : $NBODY_SPEEDUP >> $OUTPUT
    echo >> $OUTPUT
fi

if [[ -f $OUTPUT.old ]]; then
    cat $OUTPUT
    echo
    echo Past results:
    cat $OUTPUT.old
fi
