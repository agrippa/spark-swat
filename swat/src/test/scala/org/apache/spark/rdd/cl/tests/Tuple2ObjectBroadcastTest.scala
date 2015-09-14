package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.DenseVector

object Tuple2ObjectBroadcastTest extends CodeGenTest[Int, Int] {
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
        inputClassType1Name, inputClassType2Name, false)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create(
            DenseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    val broadcast : Broadcast[Array[Tuple2[Int, DenseVector]]] = null

    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        broadcast.value(in)._1 + broadcast.value(in)._2(0).toInt
      }
    }
  }
}
