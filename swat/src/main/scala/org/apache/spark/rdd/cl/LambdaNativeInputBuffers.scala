package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

class LambdaNativeInputBuffers[L: ClassTag](val nReferencedFields : Int,
        val buffers : Array[InputBufferWrapper[_]], val fieldSizes : Array[Int],
        val fieldNumArgs : Array[Int], val dev_ctx : Long)
        extends NativeInputBuffers[L] {
  val nativeBuffers : Array[NativeInputBuffers[_]] =
      new Array[NativeInputBuffers[_]](nReferencedFields)
  var tocopy : Int = -1

  for (i <- 0 until nReferencedFields) {
    nativeBuffers(i) = buffers(i).generateNativeInputBuffer(dev_ctx)
  }

  override def releaseOpenCLArrays() {
    for (b <- nativeBuffers) {
      b.releaseOpenCLArrays
    }
  }

  override def copyToDevice(startArgnum : Int, ctx : Long, dev_ctx : Long,
          cacheId : CLCacheID, persistent : Boolean) : Int = {
    var argnum : Int = startArgnum

    for (i <- 0 until nReferencedFields) {
      if (fieldSizes(i) > 0) {
        nativeBuffers(i).copyToDevice(argnum, ctx, dev_ctx, cacheId, persistent)
        cacheId.incrComponent(fieldNumArgs(i))
      } else {
        OpenCLBridge.setNullArrayArg(ctx, argnum)
      }
      argnum += fieldNumArgs(i)
    }

    argnum
  }

  override def next() : L = {
    throw new UnsupportedOperationException
  }

  override def hasNext() : Boolean = {
    throw new UnsupportedOperationException
  }
}
