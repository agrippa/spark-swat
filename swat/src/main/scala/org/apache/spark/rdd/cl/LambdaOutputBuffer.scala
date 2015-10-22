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
    val acc : NativeInputBuffers[T], val ctx : Long, val dev_ctx : Long)
    extends OutputBufferWrapper[U] {
  var anyLeft = true

  override def next() : U = {
    f(acc.next)
  }

  override def hasNext() : Boolean = {
    if (!acc.hasNext) {
      anyLeft = false
      OpenCLBridge.addFreedNativeBuffer(ctx, dev_ctx, acc.id)
    }
    anyLeft
  }

  override def countArgumentsUsed() : Int = {
    throw new java.lang.UnsupportedOperationException()
  }

  override def fillFrom(kernel_ctx : Long, nativeOutputBuffers : NativeOutputBuffers[U]) {
    throw new java.lang.UnsupportedOperationException()
  }

  override def getNativeOutputBufferInfo() : Array[Int] = {
    throw new java.lang.UnsupportedOperationException()
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : U, entryPoint : Entrypoint) :
          NativeOutputBuffers[U] = {
    throw new java.lang.UnsupportedOperationException()
  }
}
