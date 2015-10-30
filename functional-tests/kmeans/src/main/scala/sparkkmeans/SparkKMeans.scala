import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object SparkKMeans {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkKMeans cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_kmeans(args.slice(1, args.length))
        }
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        // val localIpAddress = localhost.getHostAddress
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }

    def dist(a : DenseVector, b : DenseVector) : Double = {
        val len : Int = a.size
        var i = 0
        var d : Double = 0.0
        while (i < len) {
            val diff = b(i) - a(i)
            d += (diff * diff)
            i += 1
        }
        scala.math.sqrt(d)
    }

    def run_kmeans(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkKMeans run K iters input-path use-swat?");
            return;
        }
        val sc = get_spark_context("Spark KMeans");

        val K : Int = args(0).toInt
        val iters = args(1).toInt;
        val inputPath = args(2);
        val useSwat = args(3).toBoolean

        val raw_points : RDD[DenseVector] = sc.objectFile[DenseVector](inputPath).cache
        val points = CLWrapper.cl[DenseVector](raw_points, useSwat)

        System.err.println("npartitions = " + points.partitions.length)
        System.err.println("partitions = " + points.partitioner.isEmpty)

        val samples : Array[DenseVector] = points.takeSample(false, K, 1);

        var centers = new Array[DenseVector](K)
        for (i <- 0 until K) {
            centers(i) = samples(i)
        }

        val startTime = System.currentTimeMillis
        var iter = 0
        while (iter < iters) {
            val iterStartTime = System.currentTimeMillis

            val broadcastedCenters = sc.broadcast(centers)

            val input_count = points.count
            System.err.println("input count = " + input_count)

            val classified : RDD[Tuple2[Int, DenseVector]] = points.map(point => {
                var closest_center = -1
                var closest_center_dist = -1.0

                var i = 0
                while (i < K) {
                    val d = dist(point, broadcastedCenters.value(i))
                    if (i == 0 || d < closest_center_dist) {
                        closest_center = i
                        closest_center_dist = d
                    }

                    i += 1
                }

                val closestLen : Int = broadcastedCenters.value(closest_center).size
                val copyOfClosest : Array[Double] = new Array[Double](closestLen)
                i = 0
                while (i < closestLen) {
                    copyOfClosest(i) = broadcastedCenters.value(closest_center)(i)
                    i += 1
                }

                (closest_center,
                 Vectors.dense(copyOfClosest).asInstanceOf[DenseVector])
            })

            val counts = classified.countByKey()
            for (c <- counts) {
              System.err.println("Cluster " + c._1 + " has " + c._2 + " members")
            }

            val sums : RDD[Tuple2[Int, DenseVector]] = classified.reduceByKey((a, b) => {
                val summed : Array[Double] = new Array[Double](a.size)
                var i = 0
                while (i < a.size) {
                    summed(i) = a(i) + b(i)
                    i += 1
                }
                Vectors.dense(summed).asInstanceOf[DenseVector]
            })

            val averages : RDD[Tuple2[Int, DenseVector]] = sums.map(kv => {
                val cluster_index : Int = kv._1
                val p : DenseVector = kv._2

                val averaged : Array[Double] = new Array[Double](p.size)
                var i = 0
                while (i < p.size) {
                    averaged(i) = p(i) / counts(cluster_index)
                    i += 1
                }
                (cluster_index, Vectors.dense(averaged).asInstanceOf[DenseVector])
            } )

            val newCenters : Array[Tuple2[Int, DenseVector]] = averages.collect
            broadcastedCenters.unpersist

            for (iter <- newCenters) {
                centers(iter._1) = iter._2
            }

            val iterEndTime = System.currentTimeMillis

            System.err.println("iteration " + (iter + 1) + " : " +
                    (iterEndTime - iterStartTime) + " ms")
            iter += 1
        }
        val endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime) + " ms")
    }

    def convert(args : Array[String]) {
        if (args.length != 2) {
            println("usage: SparkKMeans convert input-dir output-dir");
            return
        }
        val sc = get_spark_context("Spark KMeans Converter");

        val inputDir = args(0)
        var outputDir = args(1)
        val input = sc.textFile(inputDir)
        val converted = input.map(line => {
            val tokens = line.split(" ")
            val arr : Array[Double] = new Array[Double](3)
            arr(0) = tokens(0).toDouble
            arr(1) = tokens(1).toDouble
            arr(2) = tokens(2).toDouble
            Vectors.dense(arr).asInstanceOf[DenseVector] })
        converted.saveAsObjectFile(outputDir)
    }

}
