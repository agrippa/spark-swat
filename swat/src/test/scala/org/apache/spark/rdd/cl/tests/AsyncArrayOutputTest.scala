package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.AsyncCodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object AsyncArrayOutputTest extends AsyncCodeGenTest[Array[Double]] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function0[Array[Double]] = {
    val v : Int = 3

    new Function0[Array[Double]] {
      override def apply() : Array[Double] = {
        val arr : Array[Double] = new Array[Double](v)
        arr(0) = v
        arr
      }
    }
  }
}
