if [[ "x$SCALA_HOME" == "x" ]]; then
  echo SCALA_HOME must be set
  exit 1
fi

JARS=./target/test-classes:${APARAPI_SWAT}/com.amd.aparapi/dist/aparapi.jar:./target/swat-1.0-SNAPSHOT.jar

for f in $(find $SCALA_HOME/lib -name "*.jar"); do
  JARS="$JARS:$f"
done

# n-elements chunking
SWAT_GPU_WEIGHT=1 SWAT_CPU_WEIGHT=0 scala -classpath ${JARS} org.apache.spark.rdd.cl.SerializationPerfTests $1 $2
