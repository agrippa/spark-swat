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

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.lang.reflect.Constructor

import org.apache.spark.mllib.linalg.DenseVector

import com.amd.aparapi.internal.model.ClassModel

class ObjectNativeInputBuffers[T](val N : Int, val structSize : Int,
        val blockingCopies : Boolean, val constructor : Constructor[_],
        val classModel : ClassModel, val structMemberTypes : Option[Array[Int]],
        val structMemberOffsets : Option[Array[Long]], val dev_ctx : Long) extends NativeInputBuffers[T] {
  val clBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * structSize)
  val buffer : Long = OpenCLBridge.pin(dev_ctx, clBuffer)

  var tocopy : Int = -1
  var iter : Int = 0

  val bb : ByteBuffer = ByteBuffer.allocate(structSize)
  bb.order(ByteOrder.LITTLE_ENDIAN)

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clBuffer, dev_ctx)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean) : Int = {
    OpenCLBridge.setNativePinnedArrayArg(ctx, dev_ctx, argnum, buffer, clBuffer,
            tocopy * structSize)
    return 1
  }

  override def next() : T = {
    val new_obj : T = constructor.newInstance().asInstanceOf[T]
    bb.clear
    OpenCLBridge.copyNativeArrayToJVMArray(buffer, iter * structSize, bb.array,
            structSize)
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb,
            structMemberTypes.get, structMemberOffsets.get)
    iter += 1
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < tocopy
  }
}
