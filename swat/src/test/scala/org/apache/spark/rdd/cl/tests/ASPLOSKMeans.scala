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

package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.ScalaArrayClassModel

import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.DenseVector

import org.apache.spark.rdd.cl.PrimitiveArrayInputBufferWrapperConfig

object ASPLOSKMeans extends SyncCodeGenTest[Tuple2[DenseVector, Double], Tuple2[DenseVector, Array[Int]]] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val arrayModel = ScalaArrayClassModel.create("I")
    models.addClassModelFor(classOf[Array[_]], arrayModel)

    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create()
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)

    val inputClassType1NameInput = CodeGenUtil.cleanClassName("org.apache.spark.mllib.linalg.DenseVector")
    val inputClassType2NameInput = CodeGenUtil.cleanClassName("D")
    val tuple2ClassModelInput : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1NameInput, inputClassType2NameInput, false)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModelInput)

    val inputClassType1NameOutput = CodeGenUtil.cleanClassName("org.apache.spark.mllib.linalg.DenseVector")
    val inputClassType2NameOutput = CodeGenUtil.cleanClassName("[I")
    val tuple2ClassModelOutput : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1NameOutput, inputClassType2NameOutput, false)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModelOutput)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[Tuple2[DenseVector, Double], Tuple2[DenseVector, Array[Int]]] = {
    val bcActiveCenters : Broadcast[Array[Double]] = null
    val runs : Int = -1
    val k : Int = -1
    val dims : Int = -1

    new Function[Tuple2[DenseVector, Double], Tuple2[DenseVector, Array[Int]]] {
      override def apply(point : Tuple2[DenseVector, Double]) : Tuple2[DenseVector, Array[Int]] = {
          val thisActiveCenters = bcActiveCenters.value

          val bestCenters = new Array[Int](10) // MAX runs
          var i = 0
          while (i < runs) {
            var bestDistance = Double.PositiveInfinity
            var j = 0
            while (j < k) {
              var lowerBoundOfSqDist =
                thisActiveCenters(i * k * (dims + 1) + j * (dims + 1) + dims) - point._2
              lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
              if (lowerBoundOfSqDist < bestDistance) {
                var distance: Double = 0
                var d = 0
                while (d < dims) {
                  distance = distance + (thisActiveCenters(i * k * (dims + 1) + j * (dims + 1) + d)
                    - point._1(d)) *
                    (thisActiveCenters(i * k * (dims + 1) + j * (dims + 1) + d) - point._1(d))
                  d += 1
                }

                if (distance < bestDistance) {
                  bestDistance = distance
                  bestCenters(i) = j
                }
              }
              j += 1
            }
            i += 1
          }
          (point._1, bestCenters)
      }
    }
  }
}
