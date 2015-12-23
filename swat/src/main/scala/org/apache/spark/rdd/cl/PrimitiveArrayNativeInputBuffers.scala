package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

class PrimitiveArrayNativeInputBuffers[T: ClassTag](val vectorElementCapacity : Int,
        val vectorCapacity : Int, val blockingCopies : Boolean,
        val tiling : Int, val dev_ctx : Long, val primitiveElementSize : Int,
        val arrayStructSize : Int) extends NativeInputBuffers[T] {
  val clValuesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorElementCapacity * primitiveElementSize)
  val valuesBuffer : Long = OpenCLBridge.pin(dev_ctx, clValuesBuffer)

  val clSizesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val sizesBuffer : Long = OpenCLBridge.pin(dev_ctx, clSizesBuffer)

  val clOffsetsBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.pin(dev_ctx, clOffsetsBuffer)

  var vectorsToCopy : Int = -1
  var elementsToCopy : Int = -1

  var iter : Int = 0
  val next_buffered : Array[T] = new Array[T](tiling)
  var next_buffered_iter : Int = 0
  var n_next_buffered : Int = 0

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clValuesBuffer, dev_ctx)
    OpenCLBridge.clFree(clSizesBuffer, dev_ctx)
    OpenCLBridge.clFree(clOffsetsBuffer, dev_ctx)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean) : Int = {

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
            arrayStructSize * vectorCapacity, persistent)
    // values array
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum, valuesBuffer,
            clValuesBuffer, elementsToCopy * primitiveElementSize)
    // Sizes of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 1, sizesBuffer,
            clSizesBuffer, vectorsToCopy * 4)
    // Offsets of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 2,
            offsetsBuffer, clOffsetsBuffer, vectorsToCopy * 4)
    // Number of vectors
    OpenCLBridge.setIntArg(ctx, argnum + 3, vectorsToCopy)
    // Tiling
    OpenCLBridge.setIntArg(ctx, argnum + 4, tiling)

    return 6
  }

  override def next() : T = {
    if (next_buffered_iter == n_next_buffered) {
        next_buffered_iter = 0
        n_next_buffered = if (vectorsToCopy - iter > tiling) tiling else vectorsToCopy - iter
        if (primitiveElementSize == 8) {
          OpenCLBridge.deserializeStridedValuesFromNativeArray(
                  next_buffered.asInstanceOf[Array[java.lang.Object]],
                  n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer,
                  iter, tiling)
        } else if (primitiveElementSize == 4) {
          OpenCLBridge.deserializeStridedIndicesFromNativeArray(
                  next_buffered.asInstanceOf[Array[java.lang.Object]],
                  n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer,
                  iter, tiling)
        } else {
          throw new RuntimeException("Unsupported primitive element size = " +
                  primitiveElementSize)
        }
    }
    val result : T = next_buffered(next_buffered_iter)
    next_buffered_iter += 1
    iter += 1
    result
  }

  override def hasNext() : Boolean = {
    iter < vectorsToCopy
  }
}
