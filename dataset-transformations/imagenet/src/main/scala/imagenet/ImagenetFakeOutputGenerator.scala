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

object ImagenetFakeOutputGenerator {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: ImagenetFakeOutputGenerator <training-data> " +
                    "<output-dir> <output-dim>")
            return;
        }

        val trainingDir = args(0)
        val outputDir = args(1)
        val outputDim : Int = args(2).toInt

        val sc = get_spark_context("Imagenet Output Generator");

        val input : RDD[Tuple2[Int, DenseVector]] = sc.objectFile(trainingDir)
        val correct : RDD[Tuple2[Int, DenseVector]] = input.map(pair => {
            val rand = new java.util.Random(System.currentTimeMillis)
            val arr : Array[Double] = new Array[Double](outputDim)
            var i = 0
            while (i < outputDim) {
                arr(i) = rand.nextDouble
                i += 1
            }
            (pair._1, Vectors.dense(arr).asInstanceOf[DenseVector])
        })

        correct.saveAsObjectFile(outputDir)
        sc.stop
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }
}
