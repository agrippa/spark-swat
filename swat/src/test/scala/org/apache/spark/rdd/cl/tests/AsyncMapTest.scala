package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.AsyncCodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object AsyncMapTest extends AsyncCodeGenTest[Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function0[Int] = {
    val v : Int = 3
    new Function0[Int] {
      override def apply() : Int = {
        v * 5
      }
    }
  }
}
