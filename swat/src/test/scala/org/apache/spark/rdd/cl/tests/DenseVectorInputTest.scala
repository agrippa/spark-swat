package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.mllib.linalg.DenseVector

import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig

object DenseVectorInputTest extends CodeGenTest[DenseVector, Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    val className : String = this.getClass.getSimpleName
    scala.io.Source.fromFile(CodeGenTests.testsPath +
            className.substring(0, className.length - 1) + ".kernel").mkString
  }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create(
            DenseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[DenseVector, Double] = {
    new Function[DenseVector, Double] {
      override def apply(in : DenseVector) : Double = {
        var sum = 0.0
        var i = 0
        while (i < in.size) {
            sum += in(i)
            i += 1
        }
        sum
      }
    }
  }
}
