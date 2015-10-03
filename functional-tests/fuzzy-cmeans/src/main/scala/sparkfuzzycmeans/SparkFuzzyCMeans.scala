import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

/*
 * Based on http://www.bindichen.co.uk/post/AI/fuzzy-c-means.html
 */

object SparkFuzzyCMeans {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkFuzzyCMeans cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_fuzzy_cmeans(args.slice(1, args.length))
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

    def run_fuzzy_cmeans(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkFuzzyCMeans run K iters input-path use-swat");
            return;
        }
        val sc = get_spark_context("Spark Fuzzy CMeans");

        val K = args(0).toInt
        val iters = args(1).toInt;
        val inputPath = args(2);
        val useSwat = args(3).toBoolean

        val m = 2
        val raw_points : RDD[DenseVector] = sc.objectFile[DenseVector](inputPath)
        val npoints : Long = raw_points.count
        val samples : Array[DenseVector] = raw_points.takeSample(false, K, 1);

        val point_cluster_pairs_raw : RDD[Tuple2[Int, DenseVector]] = raw_points.flatMap(point => {
            var buffer = new ListBuffer[Tuple2[Int, DenseVector]]()
            for (i <- 0 until K) {
                buffer += new Tuple2[Int, DenseVector](i, point)
            }
            buffer.toList
        })
        point_cluster_pairs_raw.cache

        val point_cluster_pairs : RDD[(Int, DenseVector)] =
            if (useSwat) CLWrapper.cl[(Int, DenseVector)](point_cluster_pairs_raw) else point_cluster_pairs_raw

        var centers : Array[DenseVector] = new Array[DenseVector](K)
        for (i <- samples.indices) {
            centers(i) = samples(i)
        }

        val startTime = System.currentTimeMillis

        var iter = 0
        while (iter < iters) {
            val iterStartTime = System.currentTimeMillis
            val broadcastedCenters = sc.broadcast(centers)

            val memberships : RDD[(Int, DenseVector)] = point_cluster_pairs.map(pair => {
                  val center_id = pair._1
                  val center : DenseVector = broadcastedCenters.value(center_id)
                  val point : DenseVector = pair._2
                  val point_len : Int = point.size

                  val target_dist : Double = dist(point, center)

                  var sum : Double = 0.0
                  var i = 0
                  while (i < K) {
                      val d : Double = dist(point, broadcastedCenters.value(i))
                      if (d != 0.0) {
                        val ratio : Double = target_dist / d
                        sum += ratio
                      }

                      i += 1
                  }

                  // last element is reserved for the u_m value
                  val output_arr : Array[Double] = new Array[Double](point_len + 1)
                  i = 0
                  while (i < point_len) {
                    output_arr(i) = point(i)
                    i += 1
                  }

                  if (sum == 0.0f) {
                    // because target_dist == 0.0f
                    output_arr(point_len) = 1.0
                  } else {
                    val u : Double = 1 / (scala.math.pow(sum, 2 / (m - 1)))
                    val u_m : Double = scala.math.pow(u, m)
                    output_arr(point_len) = u_m
                  }
                  (center_id, Vectors.dense(output_arr).asInstanceOf[DenseVector])
                })

            val updates : RDD[Tuple2[Int, DenseVector]] =
                memberships.reduceByKey((p1, p2) => {
                  assert(p1.size == p2.size)
                  // Last element is still u_m
                  val arr : Array[Double] = new Array[Double](p1.size)
                  var i = 0
                  while (i < p1.size) {
                      arr(i) = p1(i) + p2(i)
                      i += 1
                  }
                  Vectors.dense(arr).asInstanceOf[DenseVector]
                })

            val new_clusters : RDD[(Int, DenseVector)] = updates.map(input => {
                  val point : DenseVector = input._2
                  val point_len : Int = point.size - 1
                  val divisor : Double = point(point_len)

                  val arr : Array[Double] = new Array[Double](point_len)
                  var i = 0
                  while (i < point_len) {
                      arr(i) = point(i) / divisor
                      i += 1
                  }

                  (input._1, Vectors.dense(arr).asInstanceOf[DenseVector])
                })

            val new_centers_with_ids : Array[Tuple2[Int, DenseVector]] = new_clusters.collect
            for (c_id <- new_centers_with_ids) {
                centers(c_id._1) = c_id._2
            }

            broadcastedCenters.unpersist(true)

            val iterEndTime = System.currentTimeMillis
            println("iteration " + (iter + 1) + " : " + (iterEndTime - iterStartTime) + " ms")
            iter += 1
        }

        val endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime))
    }

    def convert(args : Array[String]) {
        if (args.length != 2) {
            println("usage: SparkFuzzyCMeans convert input-dir output-dir");
            return
        }
        val sc = get_spark_context("Spark Fuzzy CMeans Converter");

        val inputDir = args(0)
        var outputDir = args(1)
        val input = sc.textFile(inputDir)
        val converted = input.map(line => {
            val tokens = line.split(" ")
            val arr : Array[Double] = new Array[Double](tokens.size)
            var i = 0
            while (i < tokens.size) {
                arr(i) = tokens(i).toDouble
                i += 1
            }

            Vectors.dense(arr).asInstanceOf[DenseVector]
        })
        converted.saveAsObjectFile(outputDir)
    }

}
