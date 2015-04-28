package org.apache.spark.rdd.cl.tests

import scala.math._
import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels

class PointWithClassifier(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }

  def classify(centers : Array[(Int, PointWithClassifier)]) : (Int, PointWithClassifier) = {
      var closest_center = -1
      var closest_center_dist = -1.0

      for (i <- 0 until centers.length) {
          val diffx = centers(i)._2.x - x
          val diffy = centers(i)._2.y - y
          val diffz = centers(i)._2.z - z
          val dist = sqrt(pow(diffx, 2) + pow(diffy, 2) + pow(diffz, 2))

          if (closest_center == -1 || dist < closest_center_dist) {
              closest_center = centers(i)._1
              closest_center_dist = dist
          }
      }

      (closest_center, new PointWithClassifier(x, y, z))
  }
}
object KMeansTest extends CodeGenTest[PointWithClassifier, (Int, PointWithClassifier)] {
  def getExpectedKernel() : String = {
      ""
  }

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

  def complete(params : LinkedList[ScalaParameter]) {
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
        in.classify(centers)
      }
    }
  }
}
