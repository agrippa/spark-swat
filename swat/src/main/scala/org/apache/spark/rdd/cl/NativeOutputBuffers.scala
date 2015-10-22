package org.apache.spark.rdd.cl

trait NativeOutputBuffers[T] {
  var id : Int = -1
  var clBuffersReadyPtr : Long = 0L

  def releaseOpenCLArrays()
}
