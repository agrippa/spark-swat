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

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

class SparseVectorOutputBufferWrapper(val N : Int, val devicePointerSize : Int,
    val heapSize : Int) extends OutputBufferWrapper[SparseVector] {
  val maxBuffers = 5
  val buffers : Array[Long] = new Array[Long](maxBuffers)

  var currSlot : Int = 0
  var nLoaded : Int = -1

  /*
   * devicePointerSize is either 4 or 8 for the values and indices in
   * SparseVector + 4 for size field + 4 for tiling field
   */
  val sparseVectorStructSize = 2 * devicePointerSize + 4 + 4
  val outArgLength = N * sparseVectorStructSize
  var outArgBuffer : Long = 0L

  override def next() : SparseVector = {
    val indices : Array[Int] = OpenCLBridge.getVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, sparseVectorStructSize, 0,
            devicePointerSize, 2 * devicePointerSize, 2 * devicePointerSize + 4,
            true).asInstanceOf[Array[Int]]
    val values : Array[Double] = OpenCLBridge.getVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, sparseVectorStructSize, devicePointerSize,
            devicePointerSize, 2 * devicePointerSize, 2 * devicePointerSize + 4,
            false).asInstanceOf[Array[Double]]

    currSlot += 1
    Vectors.sparse(values.size, indices, values).asInstanceOf[SparseVector]
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[SparseVector]) {
    currSlot = 0
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    outArgBuffer = nativeOutputBuffers.asInstanceOf[SparseVectorNativeOutputBuffers].pinnedBuffer
    OpenCLBridge.fillHeapBuffersFromKernelContext(kernel_ctx, buffers,
            maxBuffers)
  }

  override def getNativeOutputBufferInfo() : Array[Int] = {
    Array(outArgLength)
  }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : SparseVector, entryPoint : Entrypoint) :
          NativeOutputBuffers[SparseVector] = {
    new SparseVectorNativeOutputBuffers(N, outArgNum, dev_ctx, ctx, entryPoint)
  }
}
