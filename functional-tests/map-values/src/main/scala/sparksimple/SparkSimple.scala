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
import org.apache.spark.mllib.linalg.DenseVector

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
            val correct : Array[Tuple2[Int, DenseVector]] = run_simple(args.slice(1, args.length), false)
            val actual : Array[Tuple2[Int, DenseVector]] = run_simple(args.slice(1, args.length), true)
            assert(correct.length == actual.length)
            for (i <- 0 until correct.length) {
                val a : Tuple2[Int, DenseVector] = correct(i)
                val b : Tuple2[Int, DenseVector] = actual(i)
                var error : Boolean = false

                if (a._1 != b._1) {
                    System.err.println(i + " _1 expected " + a._1 +
                        " but got " + b._1)
                    error = true
                }

                if (a._2.size != b._2.size) {
                    System.err.println(i + " size mismatch, expected " +
                            a._2.size + " but got " + b._2.size)
                    error = true
                }

                val min_length = if (a._2.size < b._2.size) a._2.size else b._2.size
                for (j <- 0 until min_length) {
                    if (a._2(j) != b._2(j)) {
                        System.err.println(i + " value mismatch at index " + j +
                                ", expected " + a._2(j) + " but got " + b._2(j))
                        error = true
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

    def run_simple(args : Array[String], useSwat : Boolean) : Array[Tuple2[Int, DenseVector]] = {
        if (args.length != 1) {
            println("usage: SparkSimple run input-path");
            return new Array[Tuple2[Int, DenseVector]](0);
        }
        val sc = get_spark_context("Spark Simple");
        System.err.println("useSwat? " + useSwat)

        val inputPath = args(0)
        val inputs_raw : RDD[Tuple2[Int, DenseVector]] = sc.objectFile[Tuple2[Int, DenseVector]](inputPath).cache
        val joined_raw : RDD[Tuple2[Int, Tuple2[DenseVector, DenseVector]]] = inputs_raw.join(inputs_raw)
        val joined = CLWrapper.pairCl[Int, Tuple2[DenseVector, DenseVector]](joined_raw, useSwat)
        val outputs : RDD[Tuple2[Int, DenseVector]] = joined.mapValues(v => {
            val a = v._1
            val b = v._2
            // val size = a.size
            val size = 5
            val arr : Array[Double] = new Array[Double](size)
            var i = 0
            while (i < size) {
                arr(i) = i
                // arr(i) = a(i) * b(i)
                i += 1
            }
            Vectors.dense(arr).asInstanceOf[DenseVector]
        })
        val outputs2 : Array[Tuple2[Int, DenseVector]] = outputs.collect
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
            val len = line.toInt
            val arr : Array[Double] = new Array[Double](len)
            for (i <- 0 until arr.size) {
                arr(i) = i
            }
            Vectors.dense(arr).asInstanceOf[DenseVector]
        }).zipWithIndex.map(vec_with_index => {
            (vec_with_index._2.toInt, vec_with_index._1)
        })
        converted.saveAsObjectFile(outputDir)
    }
}
