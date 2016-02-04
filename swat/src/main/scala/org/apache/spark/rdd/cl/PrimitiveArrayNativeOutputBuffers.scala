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

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher

class PrimitiveArrayNativeOutputBuffers[T](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long,
        val entryPoint : Entrypoint) extends NativeOutputBuffers[T] {

  val clOutBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * 4)
  val pinnedOutBuffer : Long = OpenCLBridge.pin(dev_ctx, clOutBuffer)

  val clIterBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * 4)
  val pinnedIterBuffer : Long = OpenCLBridge.pin(dev_ctx, clIterBuffer)

  override def addToArgs() {
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum, clOutBuffer)
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum + 1, clIterBuffer)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clOutBuffer, dev_ctx)
    OpenCLBridge.clFree(clIterBuffer, dev_ctx)
  }
}

