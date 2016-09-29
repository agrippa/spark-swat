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
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

object SparkConnectedComponents {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkConnectedComponents cmd")
            return;
        }

        val cmd = args(0)

        val sc = get_spark_context("Spark Connected Components");

        if (cmd == "convert") {
            convert(args.slice(1, args.length), sc)
        } else if (cmd == "run") {
            val useSwat = args(1).toBoolean
            run_connected_components_cl(args.slice(2, args.length), sc, useSwat)
        } else if (cmd == "check") {
            val baseline : Array[Int] = run_connected_components_cl(args.slice(1,
                args.length), sc, false)
            val test : Array[Int] = run_connected_components_cl(args.slice(1,
                args.length), sc, true)
            assert(baseline.length == test.length)
            for (i <- baseline.indices) {
              if (baseline(i) != test(i)) {
                System.err.println("Different at index " + i + ", expected " +
                    baseline(i) + " but got " + test(i))
                System.exit(1)
              }
            }
        } else {
          System.err.println("Unknown command \"" + cmd + "\"");
          System.exit(1)
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

    def run_connected_components_cl(args : Array[String], sc : SparkContext,
            useSwat : Boolean) : Array[Int] = {
        if (args.length < 1) {
            println("usage: SparkConnectedComponents run-cl use-swat " +
                    "input-link-path [n-nodes] [limit-iters]");
            System.exit(1)
        }

        val inputLinksPath = args(0);

        val raw_edges : RDD[Tuple2[Int, Int]] = sc.objectFile[Tuple2[Int, Int]](inputLinksPath).cache
        val edges = CLWrapper.pairCl[Int, Int](raw_edges, useSwat)

        var nNodes : Long = -1
        if (args.length > 1) {
            nNodes = args(1).toLong
        } else {
            val countStart = System.currentTimeMillis
            nNodes = raw_edges.flatMap(pair => List(pair._1, pair._2)).distinct().count()
            val countElapsed = System.currentTimeMillis - countStart
            System.err.println("nNodes=" + nNodes + ", " + countElapsed +
                    " ms to count")
            System.exit(1)
        }

        val limitIters : Int = if (args.length > 2) args(2).toInt else -1

        val membership : Array[Int] = new Array[Int](nNodes.toInt)
        for (i <- membership.indices) {
            membership(i) = i
        }

        var startTime = System.currentTimeMillis
        var done = false
        var iters = 0
        do {
          val iterStartTime = System.currentTimeMillis
          val broadcastMembership = sc.broadcast(membership)

          val updates : RDD[Tuple2[Int, Int]] = edges.map(edge => {
                val component_1 = broadcastMembership.value(edge._1)
                val component_2 = broadcastMembership.value(edge._2)
                if (component_1 == component_2) {
                  // Both already the same component
                  (-1, -1)
                } else {
                    if (component_1 < component_2) {
                      (edge._2, component_1) 
                    } else {
                      (edge._1, component_2)
                    }
                }
              })

          val new_classifications : RDD[(Int, Int)] = updates.reduceByKey(
              (cluster1, cluster2) => { if (cluster1 < cluster2) cluster1 else cluster2 })
          val collected_new_classifications : Array[(Int, Int)] = new_classifications.collect

          for (classification <- collected_new_classifications) {
            if (classification._1 != -1) {
              membership(classification._1) = classification._2
            } else {
              assert(classification._2 == -1)
            }
          }

          iters += 1

          done = ((limitIters != -1 && iters >= limitIters) ||
                  collected_new_classifications.length == 1)

          // broadcastMembership.unpersist(true)
          val iterEndTime = System.currentTimeMillis

          System.err.println("iter=" + iters + ", " +
                  (iterEndTime - iterStartTime) + " ms for this iter, collected.length=" +
                  collected_new_classifications.length + ", " +
                  (System.currentTimeMillis - startTime) + " ms elapsed overall")
        } while (!done);

        var endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime) + " ms")

        val allClusters : java.util.Set[java.lang.Integer] =
            new java.util.HashSet[java.lang.Integer]()
        for (cluster <- membership) {
          allClusters.add(cluster)
        }

        System.out.println("# iters = " + iters)
        System.out.println("# clusters = " + allClusters.size())
        sc.stop
        membership
    }

    def convert(args : Array[String], sc : SparkContext) {
        if (args.length != 2) {
            println("usage: SparkConnectedComponents convert input-links-dir " +
                    "output-links-dir");
            return
        }

        val inputLinksDir = args(0)
        var outputLinksDir = args(1)

        val linksInput = sc.textFile(inputLinksDir)
        val converted = linksInput.map(line => {
            val tokens = line.split("\t")
            val a = tokens(0).toInt
            val b = tokens(1).toInt
            (a, b) })
        converted.saveAsObjectFile(outputLinksDir)
    }
}
