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

import org.apache.spark.rdd.cl.PrimitiveArrayInputBufferWrapperConfig

object ByteArrayInputTest extends SyncCodeGenTest[Array[Byte], Int] {
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

  def getFunction() : Function1[Array[Byte], Int] = {
    new Function[Array[Byte], Int] {
      override def apply(in : Array[Byte]) : Int = {
        var sum : Int = 0
        var i = 0
        while (i < in.length) {
            sum += in(i)
            i += 1
        }
        sum
      }
    }
  }
}
