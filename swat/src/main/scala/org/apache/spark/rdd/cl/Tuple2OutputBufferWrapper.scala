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

class Tuple2OutputBufferWrapper[K : ClassTag, V : ClassTag](
    val sampleOutput : Tuple2[_, _], N : Int, val entryPoint : Entrypoint,
    val devicePointerSize : Int, val heapSize : Int)
    extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

  val member0OutputBuffer : OutputBufferWrapper[K] =
        OpenCLBridgeWrapper.getOutputBufferFor[K](
        sampleOutput._1.asInstanceOf[K], N, entryPoint, devicePointerSize,
        heapSize)
  val member1OutputBuffer : OutputBufferWrapper[V] =
        OpenCLBridgeWrapper.getOutputBufferFor[V](
        sampleOutput._2.asInstanceOf[V], N, entryPoint, devicePointerSize,
        heapSize)

  override def next() : Tuple2[K, V] = {
    (member0OutputBuffer.next, member1OutputBuffer.next)
  }

  override def hasNext() : Boolean = {
    member0OutputBuffer.hasNext
  }

  override def countArgumentsUsed() : Int = {
      member0OutputBuffer.countArgumentsUsed +
          member1OutputBuffer.countArgumentsUsed
  }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[Tuple2[K, V]]) {
    val tuple2OutputBuffers = nativeOutputBuffers.asInstanceOf[Tuple2NativeOutputBuffers[K, V]]
    member0OutputBuffer.fillFrom(kernel_ctx, tuple2OutputBuffers.nestedBuffer1)
    member1OutputBuffer.fillFrom(kernel_ctx, tuple2OutputBuffers.nestedBuffer2)
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : Tuple2[K, V], entryPoint : Entrypoint) :
          NativeOutputBuffers[Tuple2[K, V]] = {
    new Tuple2NativeOutputBuffers(N, outArgNum, dev_ctx, ctx, sampleOutput,
            entryPoint, member0OutputBuffer, member1OutputBuffer)
  }
}
