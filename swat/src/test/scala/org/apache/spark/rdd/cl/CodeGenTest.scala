package org.apache.spark.rdd.cl

trait CodeGenTest {
  def getExpectedKernel() : String
  def getExpectedNumInputs() : Int
}
