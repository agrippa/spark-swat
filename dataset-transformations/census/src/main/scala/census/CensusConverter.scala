import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector

object CensusConverter {
    def main(args : Array[String]) {
        if (args.length != 2) {
            println("usage: CensusConverter input-dir output-dir")
            return;
        }

        val inputDir = args(0)
        val outputDir = args(1)
        val sc = get_spark_context("Census Converter");

        val input : RDD[String] = sc.textFile(inputDir)
        val converted : RDD[DenseVector] = input.map(line => {
            val tokens : Array[String] = line.split(",")
            var nAttributes = tokens.length - 1
            val arr : Array[Double] = new Array[Double](nAttributes)
            var i : Int = 0
            while (i < nAttributes) {
                arr(i) = tokens(i + 1).toDouble
                i += 1
            }
            Vectors.dense(arr).asInstanceOf[DenseVector]
        })
        converted.saveAsObjectFile(outputDir)
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
