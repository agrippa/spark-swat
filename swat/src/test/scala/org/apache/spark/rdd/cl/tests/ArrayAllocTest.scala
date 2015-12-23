package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.SyncCodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object ArrayAllocTest extends SyncCodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        val intArr = new Array[Int](5)
        val doubleArr = new Array[Double](2)
        in + 3
      }
    }
  }
}
