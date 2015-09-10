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
    acc : InputBufferWrapper[T]) extends OutputBufferWrapper[U] {
  def next() : U = {
    f(acc.next)
  }

  def hasNext() : Boolean = {
    acc.hasNext
  }

  def releaseBuffers(bbCache : ByteBufferCache) {
    throw new java.lang.UnsupportedOperationException()
  }

  // Returns true if all work on the device is complete
  def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
    throw new java.lang.UnsupportedOperationException()
  }

  def finish(ctx : Long, dev_ctx : Long) {
    throw new java.lang.UnsupportedOperationException()
  }

  def countArgumentsUsed() : Int = {
    throw new java.lang.UnsupportedOperationException()
  }
}
