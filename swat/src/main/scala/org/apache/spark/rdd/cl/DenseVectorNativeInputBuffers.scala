/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.apache.spark.rdd.cl

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

class DenseVectorNativeInputBuffers(val vectorElementCapacity : Int,
        val vectorCapacity : Int, val denseVectorStructSize : Int,
        val blockingCopies : Boolean, val tiling : Int, val dev_ctx : Long)
        extends NativeInputBuffers[DenseVector] {
  val clValuesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorElementCapacity * 8)
  val valuesBuffer : Long = OpenCLBridge.pin(dev_ctx, clValuesBuffer)

  val clSizesBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val sizesBuffer : Long = OpenCLBridge.pin(dev_ctx, clSizesBuffer)

  val clOffsetsBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.pin(dev_ctx, clOffsetsBuffer)

  var vectorsToCopy : Int = -1
  var elementsToCopy : Int = -1

  var iter : Int = 0
  val next_buffered : Array[Array[Double]] = new Array[Array[Double]](tiling)
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
            denseVectorStructSize * vectorCapacity, persistent)
    // values array, size of double = 8
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 1, valuesBuffer,
            clValuesBuffer, elementsToCopy * 8)
    // Sizes of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 2, sizesBuffer,
            clSizesBuffer, vectorsToCopy * 4)
    // Offsets of each vector
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum + 3,
            offsetsBuffer, clOffsetsBuffer, vectorsToCopy * 4)
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
