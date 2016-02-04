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

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

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
            val correct : Array[Double] = run_simple(args.slice(1, args.length), false)
            val actual : Array[Double] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a : Double = correct(i)
                val b : Double = actual(i)
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

    def run_simple(args : Array[String], useSwat : Boolean) : Array[Double] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            return new Array[Double](0);
        }
        val sc = get_spark_context("Spark Simple");

        val inputPath = args(0)
        val inputs_raw : RDD[Tuple2[Int, DenseVector]] = sc.objectFile[Tuple2[Int, DenseVector]](inputPath).cache
        val inputs = if (useSwat) CLWrapper.cl[Tuple2[Int, DenseVector]](inputs_raw) else inputs_raw
        inputs.cache

        val arr : Array[DenseVector] = new Array[DenseVector](5)
        for (i <- arr.indices) {
            val a = new Array[Double](5)
            a(0) = i * 0.0
            a(1) = i * 1.0
            a(2) = i * 2.0
            a(3) = i * 3.0
            a(4) = i * 4.0
            arr(i) = Vectors.dense(a).asInstanceOf[DenseVector]
        }

        val broadcasted = sc.broadcast(arr)

        var outputs : RDD[Double] = null
        var iter = 0
        while (iter < 5) {
          outputs = inputs.map(v => {
            broadcasted.value(v._1 % 5)(v._1 % 5) + v._2(0) + v._2(1) +
                v._2(2) + v._2(3) + v._2(4)
          })
          outputs.count
          iter += 1
        }
        val outputs2 : Array[Double] = outputs.collect
        broadcasted.unpersist

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
            val arr : Array[Double] = new Array[Double](5)
            val rand : java.util.Random = new java.util.Random(System.currentTimeMillis)
            arr(0) = rand.nextDouble * 10.0
            arr(1) = rand.nextDouble * 10.0
            arr(2) = rand.nextDouble * 10.0
            arr(3) = rand.nextDouble * 10.0
            arr(4) = rand.nextDouble * 10.0
            (line.toInt, Vectors.dense(arr).asInstanceOf[DenseVector]) })
        converted.saveAsObjectFile(outputDir)
    }
}
