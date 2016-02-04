/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

class Document(val id: Int, val rank: Double, val linkCount: Int)
    extends java.io.Serializable {
  def this() {
    this(0, 0.0, 0)
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

        /*
         * The convention used here is that link._1 is the destination node of a
         * link, link._2 is the source node of a link
         */
        val raw_links : RDD[Tuple2[Int, Int]] = sc.objectFile[Tuple2[Int, Int]](
                inputLinksPath).cache
        val links = CLWrapper.pairCl[Int, Int](raw_links, useSwat)

        val raw_docs : RDD[Tuple2[Double, Int]] = sc.objectFile(inputDocsPath)
        val collected_docs : Array[Tuple2[Double, Int]] = raw_docs.collect
        System.err.println("Processing " + collected_docs.length + " documents")
        val doc_ranks : Array[Double] = new Array[Double](collected_docs.length)
        val doc_link_counts : Array[Int] = new Array[Int](collected_docs.length)
        for (i <- collected_docs.indices) {
            doc_ranks(i) = collected_docs(i)._1
            doc_link_counts(i) = collected_docs(i)._2
        }

        val broadcastDocLinkCounts = sc.broadcast(doc_link_counts)

        val startTime = System.currentTimeMillis
        var iter = 0
        while (iter < iters) {
            val iterStart = System.currentTimeMillis

            val broadcastDocRanks = sc.broadcast(doc_ranks)

            val linkWeights : RDD[(Int, Double)] = links.map(link => {
                    (link._1, broadcastDocRanks.value(link._2) /
                        broadcastDocLinkCounts.value(link._2))
                })
            val newRanksRDD : RDD[(Int, Double)] = linkWeights.reduceByKey(
                (weight1, weight2) => { weight1 + weight2 })
            val newRanks : Array[(Int, Double)] = newRanksRDD.collect
            /*
             * newRanks.length may not equal doc_ranks.length if a given
             * document has no documents targeting it. In that case, its rank is
             * static and it will not be included in the updated ranks.
             */

            for (update <- newRanks) {
                doc_ranks(update._1) = update._2
            }

            iter += 1
            broadcastDocRanks.unpersist

            val iterEnd = System.currentTimeMillis
            System.err.println("iter " + iter + ", iter time=" +
                    (iterEnd - iterStart) + ", program time so far=" +
                    (iterEnd - startTime))
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
            (src, dst) })
        converted.saveAsObjectFile(outputLinksDir)

        val docsInput = sc.textFile(args(2))
        val convertedDocs = docsInput.map(line => {
            val tokens = line.split(" ")
            val rank = tokens(0).toDouble
            val nlinks = tokens(1).toInt
            (rank, nlinks) })
        convertedDocs.saveAsObjectFile(args(3))
    }

}
