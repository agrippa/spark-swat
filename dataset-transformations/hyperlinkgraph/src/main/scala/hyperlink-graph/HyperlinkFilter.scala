import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

object HyperlinkFilter {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: HyperlinkFilter input-dir output-dir percent-to-keep")
                return
        }

        val inputDir = args(0)
        val outputDir = args(1)
        val percentToKeep = args(2).toDouble
        assert(percentToKeep >= 0.0 && percentToKeep <= 1.0)

        val sc = get_spark_context("Hyperlink Filter");

        val input : RDD[Tuple2[Int, Int]] = sc.objectFile(inputDir)

        val nDocs : Long = input.flatMap(p => List(p._1, p._2)).distinct.count
        val newNDocs : Int = (nDocs.toDouble * percentToKeep).toInt
        System.err.println("original = " + nDocs + ", filtered = " + newNDocs)

        val output : RDD[Tuple2[Int, Int]] = input.filter(link => {
            assert(link._1 >= 0 && link._1 < nDocs)
            assert(link._2 >= 0 && link._2 < nDocs)
            link._1 < newNDocs && link._2 < newNDocs
        })
        output.saveAsObjectFile(outputDir)
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
