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

class LambdaOutputBuffer[T : ClassTag, U : ClassTag](f : T => U,
    acc : NativeInputBuffers[T],
    emptyNativeInputBuffers : java.util.LinkedList[NativeInputBuffers[T]])
    extends OutputBufferWrapper[U] {
  var anyLeft = true

  override def next() : U = {
    f(acc.next)
  }

  override def hasNext() : Boolean = {
    if (!acc.hasNext) {
      anyLeft = false

      emptyNativeInputBuffers.synchronized {
        emptyNativeInputBuffers.push(acc)
        emptyNativeInputBuffers.notify
      }
    }
    anyLeft
  }

  // Returns true if all work on the device is complete
  override def kernelAttemptCallback(nLoaded : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int, heapTop : Int) {
    throw new java.lang.UnsupportedOperationException()
  }

  override def finish(ctx : Long, dev_ctx : Long, outArgNum : Int, nLoaded : Int) {
    throw new java.lang.UnsupportedOperationException()
  }

  override def countArgumentsUsed() : Int = {
    throw new java.lang.UnsupportedOperationException()
  }

  override def reset() {
    throw new java.lang.UnsupportedOperationException()
  }

  override def releaseNativeArrays() {
  }
}
