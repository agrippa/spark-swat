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

import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

class SparseVectorOutputBufferWrapper(val N : Int, val devicePointerSize : Int,
    val heapSize : Int) extends OutputBufferWrapper[SparseVector] {
  val maxBuffers = 5
  val buffers : Array[Long] = new Array[Long](maxBuffers)

  var currSlot : Int = 0
  var nLoaded : Int = -1

  /*
   * devicePointerSize is either 4 or 8 for the values and indices in
   * SparseVector + 4 for size field + 4 for tiling field
   */
  val sparseVectorStructSize = 2 * devicePointerSize + 4 + 4
  val outArgLength = N * sparseVectorStructSize
  var outArgBuffer : Long = 0L

  override def next() : SparseVector = {
    val indices : Array[Int] = OpenCLBridge.getVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, sparseVectorStructSize, 0,
            devicePointerSize, 2 * devicePointerSize, 2 * devicePointerSize + 4,
            true).asInstanceOf[Array[Int]]
    val values : Array[Double] = OpenCLBridge.getVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, sparseVectorStructSize, devicePointerSize,
            devicePointerSize, 2 * devicePointerSize, 2 * devicePointerSize + 4,
            false).asInstanceOf[Array[Double]]

    currSlot += 1
    Vectors.sparse(values.size, indices, values).asInstanceOf[SparseVector]
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[SparseVector]) {
    currSlot = 0
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    outArgBuffer = nativeOutputBuffers.asInstanceOf[SparseVectorNativeOutputBuffers].pinnedBuffer
    OpenCLBridge.fillHeapBuffersFromKernelContext(kernel_ctx, buffers,
            maxBuffers)
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : SparseVector, entryPoint : Entrypoint) :
          NativeOutputBuffers[SparseVector] = {
    new SparseVectorNativeOutputBuffers(N, outArgNum, dev_ctx, ctx, entryPoint)
  }
}
