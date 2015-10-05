package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig

object Tuple2DenseOutputTest extends CodeGenTest[Int, (Int, DenseVector)] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("org.apache.spark.mllib.linalg.DenseVector")
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, true)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create()
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter("Lorg.apache.spark.mllib.linalg.DenseVector;", true)
  }

  def getFunction() : Function1[Int, (Int, DenseVector)] = {
    new Function[Int, (Int, DenseVector)] {
      override def apply(in : Int) : Tuple2[Int, DenseVector] = {
        val arr : Array[Double] = new Array[Double](in)
        var i = 0
        while (i < in) {
            arr(i) = in
            i += 1
        }
        (in, Vectors.dense(arr).asInstanceOf[DenseVector])
      }
    }
  }
}
