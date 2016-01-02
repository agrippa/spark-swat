import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector

object SparkSimple {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkSimple cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_simple(args.slice(2, args.length), args(1).toBoolean)
        } else if (cmd == "check") {
            val correct : Array[Double] = run_simple(args.slice(1, args.length), false)
            val actual : Array[Double] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a : Double = correct(i)
                val b : Double = actual(i)
                var error : Boolean = false

                if (a != b) {
                    System.err.println(i + " expected " + a + " but got " + b)
                    error = true
                }

                if (error) System.exit(1)
            }
            System.err.println("PASSED")
        }
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }

    def run_simple(args : Array[String], useSwat : Boolean) : Array[Double] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            return new Array[Double](0);
        }
        val sc = get_spark_context("Spark Simple");

        val inputPath = args(0)
        val inputs_raw : RDD[Tuple2[DenseVector, DenseVector]] =
            sc.objectFile[Tuple2[DenseVector, DenseVector]](inputPath).cache
        val inputs = CLWrapper.cl[Tuple2[DenseVector, DenseVector]](inputs_raw, useSwat)

        val outputs : RDD[Double] = inputs.map(v => {
            val v1 : DenseVector = v._1
            val v2 : DenseVector = v._2

            var i = 0
            var sum = 0.0
            while (i < v1.size) {
                sum += (v1(i) + v2(i))
                i += 1
            }
            sum
          })
        val outputs2 : Array[Double] = outputs.collect
        var i = 0
        while (i < 10) {
            System.err.println(outputs2(i))
            i += 1
        }
        System.err.println("...")
        sc.stop
        outputs2
    }

    def convert(args : Array[String]) {
        if (args.length != 2) {
            println("usage: SparkSimple convert input-dir output-dir");
            return
        }
        val sc = get_spark_context("Spark KMeans Converter");

        val inputDir = args(0)
        var outputDir = args(1)
        val input = sc.textFile(inputDir)

        val converted = input.map(line => {
            val tokens : Array[String] = line.split(" ")
            val arr1 : Array[Double] = new Array[Double](tokens.length)
            val arr2 : Array[Double] = new Array[Double](tokens.length)
            for (i <- 0 until tokens.length) {
                arr1(i) = tokens(i).toDouble
                arr2(i) = tokens(i).toDouble * 2.0
            }
            (Vectors.dense(arr1), Vectors.dense(arr2)) })
        converted.saveAsObjectFile(outputDir)
    }
}