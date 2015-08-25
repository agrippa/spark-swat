if [[ "x$SCALA_HOME" == "x" ]]; then
  echo SCALA_HOME must be set
  exit 1
fi

JARS=./target/test-classes:${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar:./target/swat-1.0-SNAPSHOT.jar

for f in $(find $SCALA_HOME/lib -name "*.jar"); do
  JARS="$JARS:$f"
done

for f in $(find $SPARK_HOME/ -name "*.jar"); do
  JARS="$JARS:$f"
done

scala -classpath ${JARS} org.apache.spark.rdd.cl.CodeGenTests $1
