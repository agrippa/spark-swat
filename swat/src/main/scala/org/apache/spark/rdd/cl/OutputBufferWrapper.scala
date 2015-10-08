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

trait OutputBufferWrapper[T] {
  def next() : T
  def hasNext() : Boolean
  /*
   * Called once per kernel attempt, must save or check any device state that
   * may be overwritten by successive kernel attempts or which controls
   * completion (e.g. anyFailed). Returns true if all work on the device is
   * complete.
   */
  def kernelAttemptCallback(nLoaded : Int, processingSucceededArgnum : Int,
          outArgNum : Int, heapArgStart : Int, heapSize : Int, ctx : Long,
          dev_ctx : Long, devicePointerSize : Int, heapTop : Int)
  /*
   * Called after all kernel attempts have completed and we know all inputs have
   * been processed.
   */
  def finish(ctx : Long, dev_ctx : Long, outArgNum : Int, nLoaded : Int)
  def countArgumentsUsed() : Int
  /*
   * Called after we have finished with the output buffer for the current inputs
   * to prepare it for future buffering.
   */
  def reset()

  def releaseNativeArrays()
}
