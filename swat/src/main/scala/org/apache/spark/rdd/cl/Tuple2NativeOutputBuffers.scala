package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels.DescMatcher

class Tuple2NativeOutputBuffers[K : ClassTag, V : ClassTag](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long,
        val sampleTuple : Tuple2[K, V], val entryPoint : Entrypoint,
        val member1OutputBuffer : OutputBufferWrapper[K],
        val member2OutputBuffer : OutputBufferWrapper[V])
        extends NativeOutputBuffers[Tuple2[K, V]] {

  val nestedBuffer1 : NativeOutputBuffers[K] =
      member1OutputBuffer.generateNativeOutputBuffer(N, outArgNum, dev_ctx, ctx,
              sampleTuple._1, entryPoint)
  val nestedBuffer2 : NativeOutputBuffers[V] =
      member2OutputBuffer.generateNativeOutputBuffer(N, outArgNum + 1, dev_ctx, ctx,
              sampleTuple._2, entryPoint)

  override def addToArgs() {
    nestedBuffer1.addToArgs
    nestedBuffer2.addToArgs
  }

  override def releaseOpenCLArrays() {
    nestedBuffer1.releaseOpenCLArrays
    nestedBuffer2.releaseOpenCLArrays
  }
}

