package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.DenseVector

object DenseVectorBroadcastTest extends CodeGenTest[Int, Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    val className : String = this.getClass.getSimpleName
    scala.io.Source.fromFile(CodeGenTests.testsPath +
            className.substring(0, className.length - 1) + ".kernel").mkString
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create(
            DenseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Double] = {
    val broadcast : Broadcast[Array[DenseVector]] = null

    new Function[Int, Double] {
      override def apply(in : Int) : Double = {
        var sum = 0.0
        var i = 0
        while (i < 5) {
          sum += broadcast.value(i)(i)
          i += 1
        }
        sum
      }
    }
  }
}
