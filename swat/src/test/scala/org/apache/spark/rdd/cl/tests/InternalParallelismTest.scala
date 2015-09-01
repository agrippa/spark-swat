package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CLWrapper
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels

object InternalParallelismTest extends CodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    val className : String = this.getClass.getSimpleName
    scala.io.Source.fromFile(CodeGenTests.testsPath +
            className.substring(0, className.length - 1) + ".kernel").mkString
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        val niters = in * 2
        CLWrapper.map(niters, (iter) => {
            val tmp = iter * 4
        })

        val out = niters / 4
        out
      }
    }
  }
}
