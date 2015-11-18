import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

object DocRankGenerator {
    def main(args : Array[String]) {
        if (args.length != 2) {
            println("usage: DocRankGenerator input-dir output-dir")
            return;
        }

        val inputDir = args(0)
        val outputDir = args(1)
        val sc = get_spark_context("Doc Rank Generator");

        val input : RDD[Tuple2[Int, Int]] = sc.objectFile(inputDir)
        val uniqueDocs : RDD[Int] = input.flatMap(p => List(p._1, p._2))
            .distinct.cache
        val countDocs = uniqueDocs.count
        System.err.println("countDocs = " + countDocs)

        val docInfo : RDD[Tuple2[Double, Int]] = uniqueDocs.map(doc => {
                val rand : java.util.Random = new java.util.Random(
                    System.currentTimeMillis)
                val rank : Double = rand.nextDouble * 100.0
                val nLinks : Int = 100 + (rand.nextInt(40) - 40)
                (rank, nLinks)
            })
        docInfo.saveAsObjectFile(outputDir)
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
