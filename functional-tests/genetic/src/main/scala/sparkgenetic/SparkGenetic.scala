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
import org.apache.spark.broadcast.Broadcast
import Array._
import scala.collection.mutable.ListBuffer
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

class NewClusterInfo(val c1 : Int, val c2 : Int) extends java.io.Serializable {
  def this() {
    this(-1, -1)
  }
}

object SparkGenetic {
    def main(args : Array[String]) {
        if (args.length != 6) {
            println("usage: SparkGenetics iters min-n-clusters " +
                    "max-n-clusters population-size input-data use-swat?")
            return
        }

        val iters = args(0).toInt
        val minNClusters = args(1).toInt
        val maxNClusters = args(2).toInt
        val populationSize = args(3).toInt
        val input = args(4)
        val useSwat = args(5).toBoolean

        System.err.println("minNClusters=" + minNClusters + " maxNClusters=" +
                maxNClusters + " populationSize=" + populationSize)

        assert(minNClusters < maxNClusters)
        assert(iters > 0)
        assert(populationSize > 0)

        val sc = get_spark_context("Spark Genetic")

        val rand : java.util.Random = new java.util.Random(1)

        /*
         * Create initial population of candidates of varying sizes by sampling
         * the input space
         */
        val raw_points : RDD[DenseVector] = sc.objectFile[DenseVector](input).cache
        val points : RDD[DenseVector] =  CLWrapper.cl(raw_points, useSwat)
        val candidateSizes : Array[Int] = new Array[Int](populationSize)
        val candidateOffsets : Array[Int] = new Array[Int](populationSize)
        var totalNClusters : Int = 0
        for (i <- 0 until populationSize) {
            candidateSizes(i) = minNClusters + rand.nextInt(maxNClusters - minNClusters)
            candidateOffsets(i) = totalNClusters
            totalNClusters += candidateSizes(i)
        }
        var populationClusters : Array[DenseVector] = points.takeSample(false,
                totalNClusters, 2)
        var clusterMembership : Array[Int] = new Array[Int](totalNClusters)
        var index = 0
        for (i <- 0 until populationSize) {
            for (j <- 0 until candidateSizes(i)) {
                clusterMembership(index) = i
                index += 1
            }
        }

        val startTime = System.currentTimeMillis
        for (iter <- 0 until iters) {
          val iterTime = System.currentTimeMillis
          val broadcastedClusters = sc.broadcast(populationClusters)
          val broadcastedMemberships = sc.broadcast(clusterMembership)
          val broadcastedCandidateSizes = sc.broadcast(candidateSizes)
          val broadcastedCandidateOffsets = sc.broadcast(candidateOffsets)

          val all_distances : RDD[DenseVector] =
              compute_min_distances_for_all_points_and_candidates(points,
                      broadcastedClusters, broadcastedMemberships,
                      populationSize, totalNClusters, sc)

          val fitness : DenseVector = all_distances.reduce((v1, v2) => {
              val arr : Array[Double] = new Array[Double](v1.size)
              var i = 0
              while (i < v1.size) {
                  arr(i) = v1(i) + v2(i)
                  i += 1
              }
              Vectors.dense(arr).asInstanceOf[DenseVector]
          })
          val fitnessArr : Array[Double] = fitness.toArray

          var nTop : Int = (populationSize * 0.2).toInt
          if (nTop * nTop < populationSize) {
            nTop = scala.math.sqrt(populationSize).toInt + 1
          }
          assert(nTop * nTop >= populationSize)
          val topCandidates : Array[Int] = new Array[Int](nTop)

          for (i <- 0 until nTop) {
              var minVal : Double = -1.0
              var minIndex : Int = -1
              for (j <- 0 until fitnessArr.length) {
                if (fitnessArr(j) >= 0.0) {
                  if (minIndex == -1 || fitnessArr(j) < minVal) {
                    minIndex = j
                    minVal = fitnessArr(j)
                  }
                }
              }
              topCandidates(i) = minIndex
              fitnessArr(minIndex) = -1.0
          }

          val broadcastedTopCandidates = sc.broadcast(topCandidates)

          val newCandidatesComponents : RDD[Tuple2[Int, NewClusterInfo]] =
            sc.parallelize((0 until nTop * nTop)).map((i) =>
              (i, broadcastedTopCandidates.value(i / nTop), broadcastedTopCandidates.value(i % nTop))).flatMap(id_s1_s2 => {
                val id : Int = id_s1_s2._1
                val s1 : Int = id_s1_s2._2
                val s2 : Int = id_s1_s2._3

                val rand : java.util.Random = new java.util.Random(id)

                val s1Size : Int = broadcastedCandidateSizes.value(s1)
                val s2Size : Int = broadcastedCandidateSizes.value(s2)
                val minSize : Int = if (s1Size < s2Size) s1Size else s2Size
                val sizeDiff : Int = if (s1Size > s2Size) s1Size - s2Size else s2Size - s1Size
                val newSetSize : Int = minSize + (if (sizeDiff > 0) rand.nextInt(sizeDiff) else 0)
                var buffer = new ListBuffer[Tuple2[Int, NewClusterInfo]]()
                for (i <- 0 until newSetSize) {
                    val c1 : Int = rand.nextInt(s1Size)
                    val c2 : Int = rand.nextInt(s2Size)
                    buffer += new Tuple2[Int, NewClusterInfo](id,
                        new NewClusterInfo(broadcastedCandidateOffsets.value(s1) + c1,
                            broadcastedCandidateOffsets.value(s2) + c2))
                }
                buffer.toList
              })

          val newCandidates : Array[Tuple2[Int, Iterable[DenseVector]]] =
            newCandidatesComponents.map(id_info => {
              val id : Int = id_info._1
              val info : NewClusterInfo = id_info._2

              val c1 : Int = info.c1
              val c2 : Int = info.c2
              val c1Vec : DenseVector = broadcastedClusters.value(c1)
              val c2Vec : DenseVector = broadcastedClusters.value(c2)

              val arr : Array[Double] = new Array[Double](c1Vec.size)
              var i = 0
              while (i < c1Vec.size) {
                arr(i) = (c1Vec(i) + c2Vec(i)) / 2.0
                i += 1
              }
              (id, Vectors.dense(arr).asInstanceOf[DenseVector])
            }).groupByKey().takeSample(false, populationSize, 1)

          totalNClusters = 0
          for (newCandidate <- newCandidates) {
            for (cluster <- newCandidate._2) {
              totalNClusters += 1
            }
          }
          System.err.println("totalNClusters = " + totalNClusters)
          populationClusters = new Array[DenseVector](totalNClusters)
          clusterMembership = new Array[Int](totalNClusters)

          var index = 0
          var candidateIndex = 0
          for (newCandidate <- newCandidates) {
            candidateOffsets(candidateIndex) = index
            for (cluster <- newCandidate._2) {
              populationClusters(index) = cluster
              clusterMembership(index) = candidateIndex
              index += 1
            }
            candidateSizes(candidateIndex) = candidateOffsets(candidateIndex) - index
            candidateIndex += 1
          }
          System.err.println("index = " + index + " candidateIndex = " + candidateIndex);

          broadcastedCandidateSizes.unpersist
          broadcastedCandidateOffsets.unpersist
          broadcastedClusters.unpersist
          broadcastedMemberships.unpersist
          broadcastedTopCandidates.unpersist

          System.err.println("iter time = " + (System.currentTimeMillis -
                      iterTime))
        }

        var i = 0
        while (clusterMembership(i) == 0) {
          val vec : DenseVector = populationClusters(i)
          System.err.print("vec " + i + " :")
          for (j <- 0 until 10) {
              System.err.print(" " + vec(j))
          }
          System.err.println()
          i += 1
        }

        System.err.println("overall time = " + (System.currentTimeMillis -
                    startTime))
    }

    def compute_min_distances_for_all_points_and_candidates(
        points : RDD[DenseVector], broadcastedClusters : Broadcast[Array[DenseVector]],
        broadcastedMemberships : Broadcast[Array[Int]], populationSize : Int,
        nclusters : Int, sc : SparkContext) : RDD[DenseVector] = {

      points.map(p => {
        val len : Int = p.size
        val arr : Array[Double] = new Array[Double](populationSize)
        var currentCandidate : Int = 0
        var i : Int = 0
        while (i < nclusters) {
          var minDist : Double = -1.0

          while (i < nclusters && broadcastedMemberships.value(i) == currentCandidate) {
            val currCluster : DenseVector = broadcastedClusters.value(i)
            val d : Double = dist(p, currCluster)
            if (minDist == -1.0 || d < minDist) {
              minDist = d
            }
            i += 1
          }
          arr(currentCandidate) = minDist
          currentCandidate += 1
        }
        Vectors.dense(arr).asInstanceOf[DenseVector]
      })
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        // val localIpAddress = localhost.getHostAddress
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }

    def dist(a : DenseVector, b : DenseVector) : Double = {
        val len : Int = a.size
        var i = 0
        var d : Double = 0.0
        while (i < len) {
            val diff = b(i) - a(i)
            d += (diff * diff)
            i += 1
        }
        scala.math.sqrt(d)
    }
}
