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

class PullCLRDDProcessor[T: ClassTag, U: ClassTag](val myNested : Iterator[T],
    val myUserLambda : T => U, val myContext: TaskContext, val myRddId : Int,
    val myPartitionIndex : Int) extends CLRDDProcessor[T, U](myNested, None,
    myUserLambda, myContext, myRddId, myPartitionIndex, true) {

  /* BEGIN READER THREAD */
  val readerRunner = new Runnable() {
    override def run() {
      assert(pullModel)

      var done : Boolean = false
      inputBuffer.setCurrentNativeBuffers(
              initiallyEmptyNativeInputBuffers.remove)

      while (!done) {
        val ioStart = System.currentTimeMillis // PROFILE
        inputBuffer.reset

        if (firstBufferOp) {
          inputBuffer.append(firstSample)
        }
        firstBufferOp = false

        inputBuffer.aggregateFrom(nested)

        val fillResult = handleFullInputBuffer()
        val nLoaded = fillResult._1
        val filled = fillResult._2

        done = !nested.hasNext && !inputBuffer.haveUnprocessedInputs
        if (done) {
          /*
           * This should come before OpenCLBridge.run to ensure there is no
           * race between setting lastSeqNo and the writer thread checking it
           * after kernel completion.
           */
          val currSeqNo : Int = OpenCLBridge.getCurrentSeqNo(ctx)
          setLastSeqNo(currSeqNo)
          inputBuffer.setCurrentNativeBuffers(null)
        }

        val doneFlag : Long = OpenCLBridge.run(ctx, dev_ctx, nLoaded,
                CLConfig.cl_local_size, lastArgIndex + 1,
                heapArgStart, CLConfig.heapsPerDevice,
                currentNativeOutputBuffer.id)
        filled.clBuffersReadyPtr = doneFlag
      }
    }
  }
  var readerThread : Thread = new Thread(readerRunner)
  readerThread.start
  /* END READER THREAD */
}
