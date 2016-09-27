#!/bin/bash

set -e

if [[ "x$SCALA_HOME" == "x" ]]; then
  echo SCALA_HOME must be set
  exit 1
fi
if [[ "x$SPARK_HOME" == "x" ]]; then
  echo SPARK_HOME must be set
  exit 1
fi

# cp ../clalloc/libclalloc.so .
# cp ../clutil/libclutil.so .

export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=../clutil:../clalloc:$LD_LIBRARY_PATH

JARS=./target/test-classes:${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar:./target/swat-1.0-SNAPSHOT.jar

for f in $(find $SCALA_HOME/lib -name "*.jar"); do
  JARS="$JARS:$f"
done

for f in $(find $SPARK_HOME/ -name "*.jar"); do
  JARS="$JARS:$f"
done

if [[ "x$SCALANLP_HOME" != "x" ]]; then
    for f in $(find $SCALANLP_HOME -name "*.jar"); do
      JARS="$JARS:$f"
    done
fi

SWAT_GPU_WEIGHT=1 SWAT_CPU_WEIGHT=0 scala -J-Xmx2g -classpath ${JARS} org.apache.spark.rdd.cl.CodeGenTests $*

# rm libclalloc.so 
# rm libclutil.so 
