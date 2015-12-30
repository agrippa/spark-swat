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
