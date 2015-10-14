package org.apache.spark.rdd.cl

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

class SparseVectorNativeInputBuffers(val vectorElementCapacity : Int,
        val vectorCapacity : Int, val sparseVectorStructSize : Int,
        val blockingCopies : Boolean, val tiling : Int, val dev_ctx : Long)
        extends NativeInputBuffers[SparseVector] {
  val clValuesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorElementCapacity * 8)
  val valuesBuffer : Long = OpenCLBridge.pinnedAlloc(dev_ctx, clValuesBuffer)

  val clIndicesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorElementCapacity * 4)
  val indicesBuffer : Long = OpenCLBridge.pinnedAlloc(dev_ctx, clIndicesBuffer)

  val clSizesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val sizesBuffer : Long = OpenCLBridge.pinnedAlloc(dev_ctx, clSizesBuffer)

  val clOffsetsBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.pinnedAlloc(dev_ctx, clOffsetsBuffer)

  var vectorsToCopy : Int = -1
  var elementsToCopy : Int = -1

  var iter : Int = 0
  val next_buffered_values : Array[Array[Double]] = new Array[Array[Double]](tiling)
  val next_buffered_indices : Array[Array[Int]] = new Array[Array[Int]](tiling)
  var next_buffered_iter : Int = 0
  var n_next_buffered : Int = 0

  override def releaseNativeArrays() {
    OpenCLBridge.unpin(valuesBuffer, clValuesBuffer, dev_ctx)
    OpenCLBridge.unpin(indicesBuffer, clIndicesBuffer, dev_ctx)
    OpenCLBridge.unpin(sizesBuffer, clSizesBuffer, dev_ctx)
    OpenCLBridge.unpin(offsetsBuffer, clOffsetsBuffer, dev_ctx)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clValuesBuffer, dev_ctx)
    OpenCLBridge.clFree(clIndicesBuffer, dev_ctx)
    OpenCLBridge.clFree(clSizesBuffer, dev_ctx)
    OpenCLBridge.clFree(clOffsetsBuffer, dev_ctx)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean) : Int = {

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
            sparseVectorStructSize * vectorCapacity, persistent)
    // indices array, size of int = 4
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 1,
            indicesBuffer, clIndicesBuffer, elementsToCopy * 4)
    // values array, size of double = 8
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 2, valuesBuffer,
            clValuesBuffer, elementsToCopy * 8)
    // Sizes of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 3, sizesBuffer,
            clSizesBuffer, vectorsToCopy * 4)
    // Offsets of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 4,
            offsetsBuffer, clOffsetsBuffer, vectorsToCopy * 4)
    // Number of vectors
    OpenCLBridge.setIntArg(ctx, argnum + 5, vectorsToCopy)
    // Tiling
    OpenCLBridge.setIntArg(ctx, argnum + 6, tiling)

    return 7
  }

  override def next() : SparseVector = {
    if (next_buffered_iter == n_next_buffered) {
        next_buffered_iter = 0
        n_next_buffered = if (vectorsToCopy - iter > tiling) tiling else vectorsToCopy - iter
        OpenCLBridge.deserializeStridedValuesFromNativeArray(
                next_buffered_values.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer, iter, tiling)
        OpenCLBridge.deserializeStridedIndicesFromNativeArray(
                next_buffered_indices.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, indicesBuffer, sizesBuffer, offsetsBuffer, iter,
                tiling)
    }
    val result : SparseVector = Vectors.sparse(
            next_buffered_indices(next_buffered_iter).size,
            next_buffered_indices(next_buffered_iter),
            next_buffered_values(next_buffered_iter)).asInstanceOf[SparseVector]
    next_buffered_iter += 1
    iter += 1
    result
  }

  override def hasNext() : Boolean = {
    iter < vectorsToCopy
  }
}
