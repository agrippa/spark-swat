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
