package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels

object ReferenceExternalObjectArrayTest extends CodeGenTest[Float, Float] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Float, Float] = {
    val arr : Array[Point] = new Array[Point](3)
    arr(0) = new Point(1.0f, 2.0f, 3.0f)
    arr(1) = new Point(4.0f, 5.0f, 6.0f)
    arr(2) = new Point(7.0f, 8.0f, 9.0f)

    new Function[Float, Float] {
      override def apply(in : Float) : Float = {
        in + arr(0).x + arr(1).y + arr(2).z
      }
    }
  }
}
