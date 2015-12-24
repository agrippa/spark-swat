package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.ScalaArrayClassModel

import org.apache.spark.rdd.cl.AsyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.rdd.cl.PrimitiveArrayInputBufferWrapperConfig

object AsyncPrimitiveArrayInputTest extends AsyncCodeGenTest[Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val arrayModel = ScalaArrayClassModel.create("D")
    models.addClassModelFor(classOf[Array[_]], arrayModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function0[Double] = {
    var arr : Array[Double] = new Array[Double](3)

    new Function0[Double] {
      override def apply() : Double = {
        var sum = 0.0
        var i = 0
        while (i < arr.length) {
            sum += arr(i)
            i += 1
        }
        sum
      }
    }
  }
}
