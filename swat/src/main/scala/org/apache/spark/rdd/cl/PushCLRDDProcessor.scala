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
