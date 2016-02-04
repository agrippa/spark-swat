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
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

class Tuple2OutputBufferWrapper[K : ClassTag, V : ClassTag](
    val sampleOutput : Tuple2[_, _], N : Int, val entryPoint : Entrypoint,
    val devicePointerSize : Int, val heapSize : Int)
    extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

  val member0OutputBuffer : OutputBufferWrapper[K] =
        OpenCLBridgeWrapper.getOutputBufferFor[K](
        sampleOutput._1.asInstanceOf[K], N, entryPoint, devicePointerSize,
        heapSize)
  val member1OutputBuffer : OutputBufferWrapper[V] =
        OpenCLBridgeWrapper.getOutputBufferFor[V](
        sampleOutput._2.asInstanceOf[V], N, entryPoint, devicePointerSize,
        heapSize)

  override def next() : Tuple2[K, V] = {
    (member0OutputBuffer.next, member1OutputBuffer.next)
  }

  override def hasNext() : Boolean = {
    member0OutputBuffer.hasNext
  }

  override def countArgumentsUsed() : Int = {
      member0OutputBuffer.countArgumentsUsed +
          member1OutputBuffer.countArgumentsUsed
  }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[Tuple2[K, V]]) {
    val tuple2OutputBuffers = nativeOutputBuffers.asInstanceOf[Tuple2NativeOutputBuffers[K, V]]
    member0OutputBuffer.fillFrom(kernel_ctx, tuple2OutputBuffers.nestedBuffer1)
    member1OutputBuffer.fillFrom(kernel_ctx, tuple2OutputBuffers.nestedBuffer2)
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : Tuple2[K, V], entryPoint : Entrypoint) :
          NativeOutputBuffers[Tuple2[K, V]] = {
    new Tuple2NativeOutputBuffers(N, outArgNum, dev_ctx, ctx, sampleOutput,
            entryPoint, member0OutputBuffer, member1OutputBuffer)
  }
}
