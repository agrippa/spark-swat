package org.apache.spark.rdd.cl.tests

import org.apache.spark.rdd.cl.CodeGenTest

object Tuple2ObjectOutputTest extends CodeGenTest[(Int, Int), (Int, Point)] {
  def getExpectedKernel() : String = {
    ""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def getFunction() : Function1[(Int, Int), (Int, Point)] = {
    new Function[(Int, Int), (Int, Point)] {
      override def apply(in : (Int, Int)) : (Int, Point) = {
        (in._2, new Point(in._1, in._1 + 1, in._1 + 2))
      }
    }
  }
}
