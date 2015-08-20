import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

class Document(val id: Int, val rank: Float, val linkCount: Int)
    extends java.io.Serializable {
  def this() {
    this(0, 0.0f, 0)
  }
}

class Link(val src: Int, val dst: Int) extends java.io.Serializable {
  def this() {
    this(0, 0)
  }
}

object SparkPageRank {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkPageRank cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_pagerank(args.slice(1, args.length))
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

    def run_pagerank(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkPageRank run iters input-link-path input-docs-path use-swat?");
            return;
        }
        val sc = get_spark_context("Spark Page Rank");

        val iters = args(0).toInt;
        val inputLinksPath = args(1);
        val inputDocsPath = args(2)
        val useSwat = args(3).toBoolean

        val raw_links : RDD[Link] = sc.objectFile(inputLinksPath)
        val links = if (useSwat) CLWrapper.cl[Link](raw_links) else raw_links

        val raw_docs : RDD[(Float, Int)] = sc.objectFile(inputDocsPath)
        val collected_docs : Array[(Float, Int)] = raw_docs.collect
        val doc_ranks : Array[Float] = new Array[Float](collected_docs.length)
        val doc_link_counts : Array[Int] = new Array[Int](collected_docs.length)
        for (i <- collected_docs.indices) {
            doc_ranks(i) = collected_docs(i)._1
            doc_link_counts(i) = collected_docs(i)._2
        }

        val broadcastDocLinkCounts = sc.broadcast(doc_link_counts)

        val startTime = System.currentTimeMillis
        var iter = 0
        while (iter < iters) {
            val broadcastDocRanks = sc.broadcast(doc_ranks)

            val linkWeights : RDD[(Int, Float)] = links.map(link => {
                    (link.dst, broadcastDocRanks.value(link.src) /
                        broadcastDocLinkCounts.value(link.src))
                })
            val newRanksRDD : RDD[(Int, Float)] = linkWeights.reduceByKey(
                (weight1, weight2) => { weight1 + weight2 })
            val newRanks : Array[(Int, Float)] = newRanksRDD.collect
            assert(newRanks.length == doc_ranks.length)

            for (update <- newRanks) {
                doc_ranks(update._1) = update._2
            }

            iter += 1
            broadcastDocRanks.unpersist
        }
        val endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime))
    }

    def convert(args : Array[String]) {
        if (args.length != 4) {
            println("usage: Spark Page Rank convert input-links-dir " +
                    "output-links-dir input-docs-file output-docs-file");
            return
        }
        val sc = get_spark_context("Spark Page Rank Converter");

        val inputLinksDir = args(0)
        var outputLinksDir = args(1)

        val linksInput = sc.textFile(inputLinksDir)
        val converted = linksInput.map(line => {
            val tokens = line.split(" ")
            val src = tokens(0).toInt
            val dst = tokens(1).toInt
            new Link(src, dst) })
        converted.saveAsObjectFile(outputLinksDir)

        val docsInput = sc.textFile(args(2))
        val convertedDocs = docsInput.map(line => {
            val tokens = line.split(" ")
            val rank = tokens(0).toFloat
            val nlinks = tokens(1).toInt
            (rank, nlinks) })
        convertedDocs.saveAsObjectFile(args(3))
    }

}
