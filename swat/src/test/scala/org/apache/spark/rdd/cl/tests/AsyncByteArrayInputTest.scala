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

object AsyncByteArrayInputTest extends AsyncCodeGenTest[Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val arrayModel = ScalaArrayClassModel.create("B")
    models.addClassModelFor(classOf[Array[_]], arrayModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function0[Int] = {
    var arr1 : Array[Byte] = new Array[Byte](3)
    var arr2 : Array[Byte] = new Array[Byte](3)

    new Function0[Int] {
      override def apply() : Int = {
        var sum : Int = 0

        var i = 0
        while (i < arr1.length) {
            sum += arr1(i) * arr2(i)
            i += 1
        }
        sum
      }
    }
  }
}
