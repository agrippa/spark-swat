package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels

object Tuple2ObjectInputPassDirectlyToFuncTest extends CodeGenTest[(Int, Point), Float] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("org.apache.spark.rdd.cl.tests.Point")

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, false)
    val models = new HardCodedClassModels()
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(0).addTypeParameter("I", false)
    params.get(0).addTypeParameter("Lorg.apache.spark.rdd.cl.tests.Point;", true)
  }

  def getFunction() : Function1[(Int, Point), Float] = {
    new Function[(Int, Point), Float] {
      override def apply(in : (Int, Point)) : Float = {
        val p : Point = in._2
        external(p.x) + externalPoint(in._2)
      }

      def external(v : Float) : Float = {
        v + 3.0f
      }

      def externalPoint(v : Point) : Float = {
        v.y + v.z
      }
    }
  }
}
