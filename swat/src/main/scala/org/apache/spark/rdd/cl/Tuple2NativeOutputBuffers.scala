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

import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels.DescMatcher

class Tuple2NativeOutputBuffers[K : ClassTag, V : ClassTag](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long,
        val sampleTuple : Tuple2[K, V], val entryPoint : Entrypoint,
        val member1OutputBuffer : OutputBufferWrapper[K],
        val member2OutputBuffer : OutputBufferWrapper[V])
        extends NativeOutputBuffers[Tuple2[K, V]] {

  val nestedBuffer1 : NativeOutputBuffers[K] =
      member1OutputBuffer.generateNativeOutputBuffer(N, outArgNum, dev_ctx, ctx,
              sampleTuple._1, entryPoint)
  val nestedBuffer2 : NativeOutputBuffers[V] =
      member2OutputBuffer.generateNativeOutputBuffer(N, outArgNum + 1, dev_ctx, ctx,
              sampleTuple._2, entryPoint)

  override def addToArgs() {
    nestedBuffer1.addToArgs
    nestedBuffer2.addToArgs
  }

  override def releaseOpenCLArrays() {
    nestedBuffer1.releaseOpenCLArrays
    nestedBuffer2.releaseOpenCLArrays
  }
}

