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

import scala.math._
import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels

class PointWithClassifier(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }

  def dist(center : PointWithClassifier) : (Float) = {
    val diffx : Float = center.x - x
    val diffy : Float = center.y - y
    val diffz : Float = center.z - z
    sqrt(diffx * diffx + diffy * diffy + diffz * diffz).asInstanceOf[Float]
  }
}

object KMeansTest extends SyncCodeGenTest[PointWithClassifier, (Int, PointWithClassifier)] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val outputClassType1Name = CodeGenUtil.cleanClassName("I")
    val outputClassType2Name = CodeGenUtil.cleanClassName(
        "org.apache.spark.rdd.cl.tests.PointWithClassifier")

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        outputClassType1Name, outputClassType2Name, true)
    val models = new HardCodedClassModels()
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter(
        "Lorg.apache.spark.rdd.cl.tests.PointWithClassifier;", true)
  }

  def getFunction() : Function1[PointWithClassifier, (Int, PointWithClassifier)] = {
    var centers = new Array[(Int, PointWithClassifier)](3)
    for (i <- 0 until 3) {
        centers(i) = (i, new PointWithClassifier(i, 2.0f * i, 3.0f * i))
    }
    new Function[PointWithClassifier, (Int, PointWithClassifier)] {
      override def apply(in : PointWithClassifier) : (Int, PointWithClassifier) = {
        var closest_center = -1
        var closest_center_dist = -1.0f

        var i = 0
        while (i < centers.length) {
          val d = in.dist(centers(i)._2)
          if (i == 0 || d < closest_center_dist) {
            closest_center = i
            closest_center_dist = d
          }

          i += 1
        }
        (centers(closest_center)._1, new PointWithClassifier(
            centers(closest_center)._2.x, centers(closest_center)._2.y,
            centers(closest_center)._2.z))
      }
    }
  }
}
