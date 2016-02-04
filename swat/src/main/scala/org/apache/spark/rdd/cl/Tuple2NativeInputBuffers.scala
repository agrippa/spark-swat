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

class Tuple2NativeInputBuffers[K : ClassTag, V : ClassTag](
        val buffer1 : InputBufferWrapper[K], val buffer2 : InputBufferWrapper[V],
        val firstMemberUsed : Boolean, val secondMemberUsed : Boolean,
        val firstMemberNumArgs : Int, val secondMemberNumArgs : Int,
        val isInput : Boolean, val tuple2StructSize : Int, val dev_ctx : Long)
        extends NativeInputBuffers[Tuple2[K, V]] {
  val member0NativeBuffers : NativeInputBuffers[K] = buffer1.generateNativeInputBuffer(dev_ctx)
  val member1NativeBuffers : NativeInputBuffers[V] = buffer2.generateNativeInputBuffer(dev_ctx)

  var tocopy : Int = -1

  override def releaseOpenCLArrays() {
    member0NativeBuffers.releaseOpenCLArrays
    member1NativeBuffers.releaseOpenCLArrays
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
