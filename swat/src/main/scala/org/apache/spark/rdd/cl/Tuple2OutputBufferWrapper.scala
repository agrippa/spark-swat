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
        sampleOutput : Tuple2[_, _], outArgNum : Int,
        nLoaded : Int, bbCache : ByteBufferCache, entryPoint : Entrypoint)
        extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

  val member0ArgStart : Int = outArgNum
  val member0OutputBuffer : OutputBufferWrapper[K] =
        OpenCLBridgeWrapper.getOutputBufferFor[K](sampleOutput._1.asInstanceOf[K], outArgNum, nLoaded, bbCache,
        entryPoint)
  val member1ArgStart : Int = outArgNum + member0OutputBuffer.countArgumentsUsed
  val member1OutputBuffer : OutputBufferWrapper[V] =
        OpenCLBridgeWrapper.getOutputBufferFor[V](sampleOutput._2.asInstanceOf[V], member1ArgStart, nLoaded,
        bbCache, entryPoint)

  override def next() : Tuple2[K, V] = {
    (member0OutputBuffer.next, member1OutputBuffer.next)
  }

  override def hasNext() : Boolean = {
    member0OutputBuffer.hasNext
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    member0OutputBuffer.releaseBuffers(bbCache)
    member1OutputBuffer.releaseBuffers(bbCache)
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
      member0OutputBuffer.kernelAttemptCallback(nLoaded, anyFailedArgNum,
              processingSucceededArgnum, member0ArgStart, heapArgStart, heapSize, ctx,
              dev_ctx, entryPoint, bbCache, devicePointerSize)
      member1OutputBuffer.kernelAttemptCallback(nLoaded, anyFailedArgNum,
              processingSucceededArgnum, member1ArgStart, heapArgStart, heapSize, ctx,
              dev_ctx, entryPoint, bbCache, devicePointerSize)
  }

  override def finish(ctx : Long, dev_ctx : Long) {
      member0OutputBuffer.finish(ctx, dev_ctx)
      member1OutputBuffer.finish(ctx, dev_ctx)
  }

  override def countArgumentsUsed() : Int = {
      member0OutputBuffer.countArgumentsUsed +
          member1OutputBuffer.countArgumentsUsed
  }
}
