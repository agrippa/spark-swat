package org.apache.spark.rdd.cl

trait InputBufferWrapper[T] {
  def append(obj : Any)
  def aggregateFrom(iter : Iterator[T]) : Int
  def hasSpace() : Boolean

  def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      broadcastId : Int, rddid : Int, partitionid : Int, offset : Int,
      component : Int) : Int
  def flush()

  // For use by LambdaOutputBuffer only
  def hasNext() : Boolean
  def next() : T
}


