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
    sampleOutput : Tuple2[_, _], N : Int,
    entryPoint : Entrypoint) extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

  val member0OutputBuffer : OutputBufferWrapper[K] =
        OpenCLBridgeWrapper.getOutputBufferFor[K](
        sampleOutput._1.asInstanceOf[K], N, entryPoint)
  val member1OutputBuffer : OutputBufferWrapper[V] =
        OpenCLBridgeWrapper.getOutputBufferFor[V](
        sampleOutput._2.asInstanceOf[V], N, entryPoint)

  override def next() : Tuple2[K, V] = {
    (member0OutputBuffer.next, member1OutputBuffer.next)
  }

  override def hasNext() : Boolean = {
    member0OutputBuffer.hasNext
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) : Boolean = {
      member0OutputBuffer.kernelAttemptCallback(nLoaded, anyFailedArgNum,
              processingSucceededArgnum, outArgNum, heapArgStart, heapSize, ctx,
              dev_ctx, devicePointerSize)
      member1OutputBuffer.kernelAttemptCallback(nLoaded, anyFailedArgNum,
              processingSucceededArgnum, outArgNum + member0OutputBuffer.countArgumentsUsed, heapArgStart, heapSize, ctx,
              dev_ctx, devicePointerSize)
  }

  override def finish(ctx : Long, dev_ctx : Long, outArgNum : Int, setNLoaded : Int) {
      member0OutputBuffer.finish(ctx, dev_ctx, outArgNum, setNLoaded)
      member1OutputBuffer.finish(ctx, dev_ctx, outArgNum, setNLoaded)
  }

  override def countArgumentsUsed() : Int = {
      member0OutputBuffer.countArgumentsUsed +
          member1OutputBuffer.countArgumentsUsed
  }

  override def reset() {
    member0OutputBuffer.reset
    member1OutputBuffer.reset
  }

  override def releaseNativeArrays() {
    member0OutputBuffer.releaseNativeArrays
    member1OutputBuffer.releaseNativeArrays
  }
}
