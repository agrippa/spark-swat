package org.apache.spark.rdd.cl

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter

trait CodeGenTest[P, R] {
  def getExpectedKernel() : String
  def getExpectedNumInputs() : Int
  def getFunction() : Function1[P, R]
  def init()
  def complete(params : LinkedList[ScalaParameter])
}
