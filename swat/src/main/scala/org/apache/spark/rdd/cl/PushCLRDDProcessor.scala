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

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

class PushCLRDDProcessor[T: ClassTag, U: ClassTag](val myUserSample : T,
    val myUserLambda : T => U, val myContext: TaskContext, val myRddId : Int,
    val myPartitionIndex : Int) extends CLRDDProcessor[T, U](null, Some(myUserSample),
    myUserLambda, myContext, myRddId, myPartitionIndex, false) {

  inputBuffer.setCurrentNativeBuffers(initiallyEmptyNativeInputBuffers.remove)
  inputBuffer.reset

  private def flush_and_run() {
    val fillResult = handleFullInputBuffer()
    val nLoaded = fillResult._1
    val filled = fillResult._2

    val doneFlag : Long = OpenCLBridge.run(ctx, dev_ctx, nLoaded,
            CLConfig.cl_local_size, lastArgIndex + 1,
            heapArgStart, CLConfig.heapsPerDevice,
            currentNativeOutputBuffer.id)
    filled.clBuffersReadyPtr = doneFlag
  }

  def push(v : T) {
    if (inputBuffer.outOfSpace) {
      flush_and_run()
    }
    inputBuffer.append(v)
  }

  def flush() {
    flush_and_run()
  }
}
