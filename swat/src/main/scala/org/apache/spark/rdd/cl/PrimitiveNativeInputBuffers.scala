package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import org.apache.spark.mllib.linalg.DenseVector

class PrimitiveNativeInputBuffers[T : ClassTag](val N : Int, val eleSize : Int,
    val blockingCopies : Boolean, val dev_ctx : Long) extends NativeInputBuffers[T] {
  val clBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * eleSize)
  val buffer : Long = OpenCLBridge.pin(dev_ctx, clBuffer)

  var tocopy : Int = -1
  var iter : Int = 0

  val chunking : Int = 100
  var remaining : Int = 0
  var tmpArrayIter : Int = 0
  val tmpArray : Array[T] = new Array[T](chunking)

  override def releaseNativeArrays() {
    OpenCLBridge.unpin(buffer, clBuffer, dev_ctx)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clBuffer, dev_ctx)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
          cacheID : CLCacheID, persistent : Boolean) : Int = {
    assert(tocopy != -1)
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum, buffer, clBuffer,
            tocopy * eleSize)
    return 1
  }

  override def next() : T = {
    if (tmpArrayIter == remaining) {
      val toBuffer = if (tocopy - iter > chunking) chunking else tocopy - iter
      OpenCLBridge.copyNativeArrayToJVMArray(buffer, iter * eleSize, tmpArray,
              toBuffer * eleSize)
      remaining = toBuffer
      tmpArrayIter = 0
    }
    val result : T = tmpArray(tmpArrayIter)
    tmpArrayIter += 1
    iter += 1
    result
  }

  override def hasNext() : Boolean = {
    iter < tocopy
  }
}
