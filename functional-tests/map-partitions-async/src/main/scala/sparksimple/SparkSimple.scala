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
            val correct : Array[Int] = run_simple(args.slice(1, args.length), false)
            val actual : Array[Int] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a = correct(i)
                val b = actual(i)
                if (a != b) {
                    System.err.println(i + ": expected=" + a + ", actual=" + b)
                    System.exit(1)
                }
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

    def run_simple(args : Array[String], useSwat : Boolean) : Array[Int] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            println(" nargs=" + args.length)
            return new Array[Int](0)
        }
        val sc = get_spark_context("Spark Simple");

        val m = 4
        val inputPath = args(0)
        val inputs_raw : RDD[Array[Byte]] = sc.objectFile[Array[Byte]](inputPath).cache

        val inputs = CLWrapper.cl[Array[Byte]](inputs_raw, useSwat)

        val outputs1 : RDD[Tuple2[Int, Option[Int]]] = inputs.mapPartitionsAsync(
                (v1_iter: Iterator[Array[Byte]], stream: AsyncOutputStream[Int, Int]) => {
                  for (v1 <- v1_iter) {
                    val v2 : Array[Byte] = new Array[Byte](v1.length)
                    for (i <- 0 until v2.length) {
                      v2(i) = (2 * v1(i)).toByte
                    }

                    stream.spawn(() => {
                      var sum : Int = 0
                      var i : Int = 0
                      while (i < v1.length) {
                        sum += v2(i) - v1(i)
                        i += 1
                      }
                      sum
                    }, None)
                  }
                })

        val outputs : Array[Int] = outputs1.map(v => v._1).collect
        sc.stop
        outputs
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
            val tokens = line.split(" ")
            val arr : Array[Byte] = new Array[Byte](tokens.length)
            for (i <- 0 until tokens.length) {
                arr(i) = tokens(i).toByte
            }
            arr
        })
        converted.saveAsObjectFile(outputDir)
    }
}
