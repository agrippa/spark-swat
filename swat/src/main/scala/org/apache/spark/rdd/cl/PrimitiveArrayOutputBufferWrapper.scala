package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

class PrimitiveArrayOutputBufferWrapper[T](val N : Int, val devicePointerSize : Int,
    val heapSize : Int, val sample : T) extends OutputBufferWrapper[T] {
  val maxBuffers : Int = 5
  val buffers : Array[Long] = new Array[Long](maxBuffers)

  var currSlot : Int = 0
  var nLoaded : Int = -1

  val primitiveClass = sample.asInstanceOf[Array[_]](0).getClass
  val primitiveTypeId : Int = if (primitiveClass.equals(classOf[java.lang.Integer])) {
            1337
          } else if (primitiveClass.equals(classOf[java.lang.Float])) {
            1338
          } else if (primitiveClass.equals(classOf[java.lang.Double])) {
            1339
          } else {
              throw new RuntimeException("Unsupported type " + primitiveClass.getName)
          }

  var outArgBuffer : Long = 0L
  var iterBuffer : Long = 0L

  override def next() : T = {
    val values = OpenCLBridge.getArrayValuesFromOutputBuffers(
            buffers, outArgBuffer, iterBuffer, currSlot, primitiveTypeId)
        .asInstanceOf[T]
    currSlot += 1
    values
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def countArgumentsUsed() : Int = { 2 }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[T]) {
    currSlot = 0
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    val natives : PrimitiveArrayNativeOutputBuffers[T] =
        nativeOutputBuffers.asInstanceOf[PrimitiveArrayNativeOutputBuffers[T]]
    outArgBuffer = natives.pinnedOutBuffer
    iterBuffer = natives.pinnedIterBuffer
    OpenCLBridge.fillHeapBuffersFromKernelContext(kernel_ctx, buffers,
            maxBuffers)
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : T, entryPoint : Entrypoint) :
          NativeOutputBuffers[T] = {
    new PrimitiveArrayNativeOutputBuffers(N, outArgNum, dev_ctx, ctx,
            entryPoint)
  }
}
