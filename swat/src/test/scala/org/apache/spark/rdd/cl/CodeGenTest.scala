package org.apache.spark.rdd.cl

trait CodeGenTest[P, R] {
  def getExpectedKernel() : String
  def getExpectedNumInputs() : Int
  def getFunction() : Function1[P, R]
}
