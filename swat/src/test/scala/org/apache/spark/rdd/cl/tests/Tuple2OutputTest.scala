package org.apache.spark.rdd.cl.tests

import org.apache.spark.rdd.cl.CodeGenTest

object Tuple2OutputTest extends CodeGenTest[(Int, Int), (Int, Int)] {
  def getExpectedKernel() : String = {
    ""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def getFunction() : Function1[(Int, Int), (Int, Int)] = {
    new Function[(Int, Int), (Int, Int)] {
      override def apply(in : (Int, Int)) : (Int, Int) = {
        (in._2, in._1)
      }
    }
  }
}
