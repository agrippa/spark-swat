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

import java.nio.BufferOverflowException
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.writer.KernelWriter

import java.nio.ByteBuffer

class PrimitiveInputBufferWrapper[T: ClassTag](val N : Int,
    val blockingCopies : Boolean)
    extends InputBufferWrapper[T]{
  val arr : Array[T] = new Array[T](N)
  val eleSize : Int = if (arr.isInstanceOf[Array[Double]]) 8 else 4
  var filled : Int = 0
  var used : Int = -1

  var nativeBuffers : PrimitiveNativeInputBuffers[T] = null

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[PrimitiveNativeInputBuffers[T]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[T] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[PrimitiveNativeInputBuffers[T]]
  }

  override def flush() { }

  override def append(obj : Any) {
    arr(filled) = obj.asInstanceOf[T]
    filled += 1
  }

  override def aggregateFrom(iter : Iterator[T]) {
    while (filled < arr.length && iter.hasNext) {
        arr(filled) = iter.next
        filled += 1
    }
  }

  override def nBuffered() : Int = {
    filled
  }

  override def countArgumentsUsed : Int = { 1 }

  override def haveUnprocessedInputs : Boolean = {
    filled > 0
  }

  override def outOfSpace : Boolean = {
    filled >= N
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val tocopy = if (limit == -1) filled else limit
    assert(tocopy <= filled)

    OpenCLBridge.copyJVMArrayToNativeArray(nativeBuffers.buffer, 0, arr, 0,
            tocopy * eleSize)
    assert(nativeBuffers.tocopy == -1)
    nativeBuffers.tocopy = tocopy
  }

  override def transferOverflowTo(otherAbstract : NativeInputBuffers[_]) :
      NativeInputBuffers[T] = {
    // setupNativeBuffersForCopy must have been called beforehand
    assert(nativeBuffers.tocopy != -1)

    val other : PrimitiveNativeInputBuffers[T] =
        otherAbstract.asInstanceOf[PrimitiveNativeInputBuffers[T]]
    val leftover = filled - nativeBuffers.tocopy

    if (leftover > 0) {
      System.arraycopy(arr, nativeBuffers.tocopy, arr, 0, leftover)
    }
    other.tocopy = -1

    filled = leftover

    val oldBuffers = nativeBuffers
    nativeBuffers = other
    return oldBuffers
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[T] = {
    new PrimitiveNativeInputBuffers(N, eleSize, blockingCopies, dev_ctx)
  }

  override def reset() { }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 1, persistent)) {
      return 1
    } else {
      return -1
    }
  }
}
