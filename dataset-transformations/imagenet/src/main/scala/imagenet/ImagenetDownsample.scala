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

object ImagenetDownsample {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: ImagenetDownsample input-dir output-dir downsample")
            return;
        }

        val inputDir = args(0)
        val outputDir = args(1)
        val downsample = args(2).toInt
        val sc = get_spark_context("Imagenet Downsample");

        val input : RDD[Tuple2[Int, DenseVector]] = sc.objectFile(inputDir)
        val sampled : RDD[Tuple2[Int, DenseVector]] = input.map(in => {
            val newLen : Int = (in._2.size + downsample - 1) / downsample
            val newArr : Array[Double] = new Array[Double](newLen)
            var i = 0
            while (i < in._2.size) {
                newArr(i / downsample) = in._2(i)
                i += downsample
            }
            (in._1, Vectors.dense(newArr).asInstanceOf[DenseVector])
        })

        sampled.saveAsObjectFile(outputDir)
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
