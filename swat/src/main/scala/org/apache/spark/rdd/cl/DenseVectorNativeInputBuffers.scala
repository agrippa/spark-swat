package org.apache.spark.rdd.cl

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

class DenseVectorNativeInputBuffers(val vectorElementCapacity : Int,
        val vectorCapacity : Int, val denseVectorStructSize : Int,
        val blockingCopies : Boolean, val tiling : Int)
        extends NativeInputBuffers[DenseVector] {
  val valuesBuffer : Long = OpenCLBridge.nativeMalloc(vectorElementCapacity * 8)
  val sizesBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)

  var vectorsToCopy : Int = -1
  var elementsToCopy : Int = -1

  var iter : Int = 0
  val next_buffered : Array[Array[Double]] = new Array[Array[Double]](tiling)
  var next_buffered_iter : Int = 0
  var n_next_buffered : Int = 0

  override def releaseNativeArrays() {
    OpenCLBridge.nativeFree(valuesBuffer)
    OpenCLBridge.nativeFree(sizesBuffer)
    OpenCLBridge.nativeFree(offsetsBuffer)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean) : Int = {

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
            denseVectorStructSize * vectorCapacity, persistent)
    // values array, size of double = 8
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 1, valuesBuffer,
        elementsToCopy * 8, cacheID.broadcast, cacheID.rdd, cacheID.partition,
        cacheID.offset, cacheID.component, persistent, blockingCopies)
    // Sizes of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 2, sizesBuffer, vectorsToCopy * 4,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 1, persistent, blockingCopies)
    // Offsets of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 3, offsetsBuffer, vectorsToCopy * 4,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 2, persistent, blockingCopies)
    // Number of vectors
    OpenCLBridge.setIntArg(ctx, argnum + 4, vectorsToCopy)
    // Tiling
    OpenCLBridge.setIntArg(ctx, argnum + 5, tiling)

    return 6
  }

  override def next() : DenseVector = {
    if (next_buffered_iter == n_next_buffered) {
        next_buffered_iter = 0
        n_next_buffered = if (vectorsToCopy - iter > tiling) tiling else vectorsToCopy - iter
        OpenCLBridge.deserializeStridedValuesFromNativeArray(
                next_buffered.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer, iter, tiling)
    }
    val result : DenseVector = Vectors.dense(next_buffered(next_buffered_iter))
        .asInstanceOf[DenseVector]
    next_buffered_iter += 1
    iter += 1
    result
  }

  override def hasNext() : Boolean = {
    iter < vectorsToCopy
  }
}
