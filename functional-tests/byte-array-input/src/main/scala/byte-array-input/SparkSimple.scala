import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

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
            val correct : Array[Int] = run_simple(args.slice(1, args.length), false)
            val actual : Array[Int] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a : Int = correct(i)
                val b : Int = actual(i)
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

    def run_simple(args : Array[String], useSwat : Boolean) : Array[Int] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            return new Array[Int](0);
        }
        val sc = get_spark_context("Spark Simple");

        val inputPath = args(0)
        val inputs_raw : RDD[Array[Byte]] = sc.objectFile[Array[Byte]](inputPath).cache
        val inputs = CLWrapper.cl[Array[Byte]](inputs_raw, useSwat)

        val outputs : RDD[Int] = inputs.map(v => {
            var i = 0
            var sum : Int = 0
            while (i < v.length) {
                sum += v(i)
                i += 1
            }
            sum
          })
        val outputs2 : Array[Int] = outputs.collect
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
            val arr : Array[Byte] = new Array[Byte](tokens.length)
            for (i <- 0 until tokens.length) {
                arr(i) = tokens(i).toByte
            }
            arr
        })
        converted.saveAsObjectFile(outputDir)
    }
}
