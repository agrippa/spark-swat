spark-submit --class SparkKMeans --master spark://localhost:7077 \
        ${SWAT_HOME}/spark-example/target/sparkkmeans-0.0.0.jar convert \
        hdfs://$(hostname):54310/input hdfs://$(hostname):54310/converted
