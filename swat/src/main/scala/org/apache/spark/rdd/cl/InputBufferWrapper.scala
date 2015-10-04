package org.apache.spark.rdd.cl

import com.amd.aparapi.internal.model.Entrypoint

/*
 * The interface presetnted by all input buffers. Input buffers aggregate input
 * items from the input stream of a particular partition and prepare them for
 * one of two modes of execution. You can access an input buffer through its
 * next/hasNext API, using it just like an iterator. This case is used when an
 * OOM error during kernel setup prevents OpenCL execution, forcing us to revert
 * to JVM execution. An input buffer can also be used to copy to an OpenCL
 * device in preparation for launching a parallel kernel on its contents.
 */
trait InputBufferWrapper[T] {
  // Add a single object to the input buffer
  def append(obj : Any)
  /*
   * Add many objects from the provided iterator, may use append behind the
   * scenes. The number of objects added is limited by the number of objects
   * accessible through iter and the available space to store them in this input
   * buffer.
   */
  def aggregateFrom(iter : Iterator[T])

  // Transfer the aggregated input items to an OpenCL device
  def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheId : CLCacheID) : Int
  // Ensure as many stored items as possible are serialized
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
  /*
   * Returns true when an input buffer has been filled to the point where it can
   * accept no more elements.
   */
  def outOfSpace : Boolean

  def releaseNativeArrays
  def nBuffered() : Int
  def reset()

  // Returns # of arguments used
  def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint) : Int
}
