package org.apache.spark.rdd.cl

trait InputBufferWrapper[T] {
  def append(obj : T)
  def aggregateFrom(iter : Iterator[T]) : Int
  def hasSpace() : Boolean
  def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int
  def flush()
}


