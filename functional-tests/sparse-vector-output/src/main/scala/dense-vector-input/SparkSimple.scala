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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.SparseVector

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
            val correct : Array[SparseVector] = run_simple(args.slice(1, args.length), false)
            val actual : Array[SparseVector] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a : SparseVector = correct(i)
                val b : SparseVector = actual(i)
                var error : Boolean = false

                if (a.size != b.size) {
                    System.err.println(i + " expected length " + a.size + " but got length " + b.size)
                    error = true
                }

                if (!error) {
                    var j = 0
                    while (j < a.size) {
                        if (a.indices(j) != b.indices(j)) {
                            System.err.println(i + " at index " + j +
                                    ", expected " + a.indices(j) + " but got " + b.indices(j))
                            error = true
                        }
                        if (a.values(j) != b.values(j)) {
                            System.err.println(i + " at value " + j +
                                    ", expected " + a.values(j) + " but got " + b.values(j))
                            error = true
                        }

                        j += 1
                    }
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

    def run_simple(args : Array[String], useSwat : Boolean) : Array[SparseVector] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            return new Array[SparseVector](0);
        }
        val sc = get_spark_context("Spark Simple");

        val inputPath = args(0)
        val inputs_raw : RDD[Int] = sc.objectFile[Int](inputPath).cache
        val inputs = if (useSwat) CLWrapper.cl[Int](inputs_raw) else inputs_raw

        val outputs : RDD[SparseVector] = inputs.map(v => {
            val indices = new Array[Int](v)
            val values = new Array[Double](v)

            var i = 0
            while (i < v) {
              indices(i) = v * i
              values(i) = v * 2 * i
              i += 1
            }
            Vectors.sparse(v, indices, values).asInstanceOf[SparseVector]
          })
        val outputs2 : Array[SparseVector] = outputs.collect
        var i = 0
        while (i < 10) {
          val curr = outputs2(i)
          var j = 0
          while (j < curr.size) {
            System.err.print("[" + curr.indices(j) + " : " + curr.values(j) + "] " )
            j += 1
          }
          System.err.println
          i += 1
        }
        System.err.println("...")
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
            line.toInt })
        converted.saveAsObjectFile(outputDir)
    }
}
