package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels

import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil

object Tuple2OutputTest extends SyncCodeGenTest[Int, (Int, Int)] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val outputClassType1Name = CodeGenUtil.cleanClassName("I")
    val outputClassType2Name = CodeGenUtil.cleanClassName("I")

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        outputClassType1Name, outputClassType2Name, true)
    val models = new HardCodedClassModels()
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter("I", false)
  }

  def getFunction() : Function1[Int, (Int, Int)] = {
    new Function[Int, (Int, Int)] {
      override def apply(in : Int) : (Int, Int) = {
        (in + 1, in + 2)
      }
    }
  }
}
