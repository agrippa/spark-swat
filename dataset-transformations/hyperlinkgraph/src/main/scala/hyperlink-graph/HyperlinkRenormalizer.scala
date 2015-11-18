import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

object HyperlinkRenormalizer {
    def main(args : Array[String]) {
        if (args.length != 2) {
            println("usage: HyperlinkRenormalizer input-dir output-dir")
            return;
        }

        val inputDir = args(0)
        val outputDir = args(1)

        val sc = get_spark_context("Hyperlink Graph Normalizer");

        val input : RDD[Tuple2[Int, Int]] = sc.objectFile(inputDir)
        val unique : RDD[Int] = input.flatMap(p => List(p._1, p._2)).distinct()
        val pairedWithId : RDD[Tuple2[Int, Long]] = unique.zipWithIndex()

        val idArray : Array[Tuple2[Int, Long]] = pairedWithId.collect
        val nNodes : Long = idArray.size
        System.out.println("nNodes = " + nNodes)
        // From old document ID to normalized ID
        val idMap : java.util.Map[Int, Int] = new java.util.HashMap[Int, Int]()
        for (pair <- idArray) {
          idMap.put(pair._1, pair._2.toInt)
        }

        val idMapBroadcasted = sc.broadcast(idMap)

        val normalized : RDD[Tuple2[Int, Int]] = input.map(pair => {
            (idMapBroadcasted.value.get(pair._1), idMapBroadcasted.value.get(pair._2))
        })
        normalized.saveAsObjectFile(outputDir)
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
