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

class Point(val x: Float, val y: Float, val z: Float, val mass: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f, 0.0f)
  }
}

class Triple(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }
}

object SparkNBody {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkNBody cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_nbody(args.slice(1, args.length))
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

    def run_nbody(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkNBody run iters input-path input-pairs-path use-swat?");
            return;
        }
        val sc = get_spark_context("Spark NBody");

        val iters = args(0).toInt;
        val inputPath = args(1);
        val inputPairsPath = args(2)
        val useSwat = args(3).toBoolean

        val points_rdd : RDD[Point] = sc.objectFile(inputPath)
        var points : Array[Point] = points_rdd.collect
        val velocities : Array[Triple] = new Array[Triple](points.length)
        for (i <- velocities.indices) {
          velocities(i) = new Triple(0.0f, 0.0f, 0.0f)
        }
        System.err.println("npoints = " + points.length)

        val raw_pairs : RDD[(Int, Int)] = sc.objectFile(inputPairsPath)
        val pairs = if (useSwat) CLWrapper.cl[(Int, Int)](raw_pairs) else raw_pairs
        pairs.cache

        val startTime = System.currentTimeMillis
        var iter = 0
        while (iter < iters) {
            val iterStartTime = System.currentTimeMillis
            val broadcastedPoints = sc.broadcast(points)
            val broadcastedVelocities = sc.broadcast(velocities)

            val accel = pairs.map(pair => {
                val target : Int = pair._1
                val actor : Int = pair._2

                val rx : Float = broadcastedPoints.value(target).x -
                    broadcastedPoints.value(actor).x
                val ry : Float = broadcastedPoints.value(target).y -
                    broadcastedPoints.value(actor).y
                val rz : Float = broadcastedPoints.value(target).z -
                    broadcastedPoints.value(actor).z

                val distSqr : Float = rx * rx + ry * ry + rz * rz
                val distSixth : Float = distSqr * distSqr * distSqr
                val invDistCube : Float = 1.0f / scala.math.sqrt(distSixth).asInstanceOf[Float]

                val s : Float = broadcastedPoints.value(actor).mass * invDistCube

                (target, new Triple(rx * s, ry * s, rz * s) )
              })

            val mergedAccel = accel.reduceByKey((a, b) => {
                new Triple(a.x + b.x, a.y + b.y, a.z + b.z)
              })

            val vel = mergedAccel.map(point => {
                val id = point._1
                val accel = point._2

                (id, new Triple(broadcastedVelocities.value(id).x + accel.x,
                                broadcastedVelocities.value(id).y + accel.y,
                                broadcastedVelocities.value(id).z + accel.z))
              })

            val collectedVelocities : Array[Tuple2[Int, Triple]] = vel.collect
            for (i <- collectedVelocities) {
              velocities(i._1) = i._2
            }

            val newPos = vel.map(velocity => {
                val id = velocity._1
                val vel = velocity._2

                new Point(broadcastedPoints.value(id).x + vel.x,
                               broadcastedPoints.value(id).y + vel.y,
                               broadcastedPoints.value(id).z + vel.z,
                               broadcastedPoints.value(id).mass)
              })

            points = newPos.collect

            broadcastedPoints.unpersist(true)
            broadcastedVelocities.unpersist(true)
            val iterEndTime = System.currentTimeMillis
            System.err.println("iteration " + (iter + 1) + " : " + (iterEndTime - iterStartTime) + " ms")

            iter += 1
        }
        val endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime))
    }

    def convert(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkNBody convert input-dir output-dir input-pairs-dir output-pairs-dir");
            return
        }
        val sc = get_spark_context("Spark NBody Converter");

        val inputDir = args(0)
        var outputDir = args(1)
        val inputPairsDir = args(2)
        val outputPairsDir = args(3)

        val input = sc.textFile(inputDir)
        input.map(line => {
            val tokens = line.split(" ")
            val x = tokens(0).toFloat
            val y = tokens(1).toFloat
            val z = tokens(2).toFloat
            val mass = tokens(3).toFloat
            new Point(x, y, z, mass) }).saveAsObjectFile(outputDir)

        val inputPairs = sc.textFile(inputPairsDir)
        inputPairs.map(line => {
            val tokens = line.split(" ")
            val p1 = tokens(0).toInt
            val p2 = tokens(1).toInt
            (p1, p2) }).saveAsObjectFile(outputPairsDir)
    }

}
