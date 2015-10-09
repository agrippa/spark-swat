package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import org.apache.spark.mllib.linalg.DenseVector

class Tuple2NativeInputBuffers[K : ClassTag, V : ClassTag](
        val buffer1 : InputBufferWrapper[K], val buffer2 : InputBufferWrapper[V],
        val firstMemberUsed : Boolean, val secondMemberUsed : Boolean,
        val firstMemberNumArgs : Int, val secondMemberNumArgs : Int,
        val isInput : Boolean, val tuple2StructSize : Int)
        extends NativeInputBuffers[Tuple2[K, V]] {
  val member0NativeBuffers : NativeInputBuffers[K] = buffer1.generateNativeInputBuffer
  val member1NativeBuffers : NativeInputBuffers[V] = buffer2.generateNativeInputBuffer

  var tocopy : Int = -1

  override def releaseNativeArrays() {
    member0NativeBuffers.releaseNativeArrays
    member1NativeBuffers.releaseNativeArrays
  }

  override def copyToDevice(startArgnum : Int, ctx : Long, dev_ctx : Long,
          cacheId : CLCacheID, persistent : Boolean) : Int = {
    if (firstMemberUsed) {
        member0NativeBuffers.copyToDevice(startArgnum, ctx, dev_ctx, cacheId, persistent)
        cacheId.incrComponent(firstMemberNumArgs)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum)
    }

    if (secondMemberUsed) {
        member1NativeBuffers.copyToDevice(startArgnum + firstMemberNumArgs, ctx, dev_ctx,
                cacheId, persistent)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum + firstMemberNumArgs)
    }

    if (isInput) {
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx,
              startArgnum + firstMemberNumArgs + secondMemberNumArgs,
              tuple2StructSize * tocopy, persistent)
      return firstMemberNumArgs + secondMemberNumArgs + 1
    } else {
      return firstMemberNumArgs + secondMemberNumArgs
    }
  }

  override def next() : Tuple2[K, V] = {
    (member0NativeBuffers.next, member1NativeBuffers.next)
  }

  override def hasNext() : Boolean = {
    member0NativeBuffers.hasNext && member1NativeBuffers.hasNext
  }
}
