package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CLWrapper
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels

object DisableInternalParallelismTest extends CodeGenTest[Int, Int] {
  override def shouldEnableNested() : Boolean = { return false }

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
        val niters = in * 2
        val arr : Array[Double] = new Array[Double](5)

        CLWrapper.map(niters, (iter) => {
            val tmp = iter * 4
            arr(tmp) = tmp
        })

        val out = niters / 4
        out
      }
    }
  }

}
