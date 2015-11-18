import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source
import scala.collection.mutable.ListBuffer

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector

object ImagenetMultiplier {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: ImagenetMultiplier input-dir output-dir multiplier")
            return;
        }

        val inputDir = args(0)
        val outputDir = args(1)
        val multiplier = args(2).toInt
        val sc = get_spark_context("Imagenet Multiplier");

        val input : RDD[Tuple2[Int, DenseVector]] = sc.objectFile(inputDir)
        val ninputs : Int = input.count.toInt

        val expanded : RDD[Tuple2[Int, DenseVector]] = input.flatMap(in => {
          var buffer = new ListBuffer[Tuple2[Int, DenseVector]]()
          for (i <- 0 until multiplier) {
            val copy : Array[Double] = new Array[Double](in._2.size)
            for (j <- 0 until in._2.size) {
              copy(j) = in._2(j)
            }
            buffer += new Tuple2[Int, DenseVector](in._1 + (i * ninputs), Vectors.dense(copy).asInstanceOf[DenseVector])
          }
          buffer.toList
        })
        expanded.saveAsObjectFile(outputDir)
        sc.stop
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        // val localIpAddress = localhost.getHostAddress
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }
}
