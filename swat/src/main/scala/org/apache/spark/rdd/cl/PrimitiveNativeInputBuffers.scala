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
