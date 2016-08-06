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

object ASPLOSValueOfRisk extends SyncCodeGenTest[Int, Array[Float]] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val arrayModel = ScalaArrayClassModel.create("F")
    models.addClassModelFor(classOf[Array[_]], arrayModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[Int, Array[Float]] = {
    val broadcastInstruments : Broadcast[Array[Array[Float]]] = null
    val numTrials : Int = -1

    new Function[Int, Array[Float]] {
      override def apply(in : Int) : Array[Float] = {
        val insts = broadcastInstruments.value
        val trialValues = new Array[Float](numTrials)
        var i = 0
        while (i < numTrials) {
          val trial = random.toFloat
          var totalValue = 0.0f
          var j = 0
          while (j < numInst) { // Instrument #
            var k = 0
            while (k < numInstValue) { // Instrument value #
              var instTrailValue = trial * insts(j)(2 + k)
              var v = if (instTrailValue > insts(j)(0)) {
                instTrailValue
              }
              else {
                insts(j)(0)
              }
              if (v < insts(j)(1))
                totalValue += v
              else
                totalValue += insts(j)(1)
              k += 1
            }
            j += 1
          }
          trialValues(i) = totalValue
          i += 1
        }
        trialValues

      }
    }
  }
}
