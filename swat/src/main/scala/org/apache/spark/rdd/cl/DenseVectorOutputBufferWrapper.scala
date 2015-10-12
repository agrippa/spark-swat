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

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

class DenseVectorOutputBufferWrapper(val N : Int, val devicePointerSize : Int,
    val heapSize : Int) extends OutputBufferWrapper[DenseVector] {
  val maxBuffers : Int = 5
  val buffers : Array[Long] = new Array[Long](maxBuffers)

  var currSlot : Int = 0
  var nLoaded : Int = -1

  /*
   * devicePointerSize is either 4 or 8 for the pointer in DenseVector + 4 for
   * size field + 4 for tiling field
   */
  val denseVectorStructSize = devicePointerSize + 4 + 4
  val outArgLength = N * denseVectorStructSize
  var outArgBuffer : Long = 0L

  override def next() : DenseVector = {
    val values : Array[Double] = OpenCLBridge.getVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, denseVectorStructSize, 0,
            devicePointerSize, devicePointerSize, devicePointerSize + 4, false)
        .asInstanceOf[Array[Double]]
    currSlot += 1
    Vectors.dense(values).asInstanceOf[DenseVector]
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def fillFrom(kernel_ctx : Long, outArgNum : Int) {
    currSlot = 0
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    outArgBuffer = OpenCLBridge.findNativeArray(kernel_ctx, outArgNum)
    OpenCLBridge.fillHeapBuffersFromKernelContext(kernel_ctx, buffers,
            maxBuffers)
  }

  override def getNativeOutputBufferInfo() : Array[Int] = {
    Array(outArgLength)
  }
}
