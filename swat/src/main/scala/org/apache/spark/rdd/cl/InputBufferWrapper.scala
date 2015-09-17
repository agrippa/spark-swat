package org.apache.spark.rdd.cl

trait InputBufferWrapper[T] {
  def append(obj : Any)
  def aggregateFrom(iter : Iterator[T])

  def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheId : CLCacheID) : Int
  def flush()

  /*
   * For use by LambdaOutputBuffer only when reverting to JVM execution due to
   * OOM errors on the accelerator.
   */
  def hasNext() : Boolean
  def next() : T

  /*
   * Used to check if an input buffer has read items from the parent partition
   * but not copied them to the device yet. For many buffer types this will
   * always return false. For dense vector or sparse vector buffers, may return
   * true if they read a vector but do not have space in their byte buffers to
   * store it.
   */
  def haveUnprocessedInputs : Boolean

  def releaseNativeArrays
  def nBuffered() : Int
  def reset()
}
