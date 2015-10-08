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
  var nFilledBuffers : Int = 0
  val maxBuffers = 5
  val buffers : Array[Long] = new Array[Long](maxBuffers)
  val bufferSizes : Array[Long] = new Array[Long](maxBuffers)
  for (i <- 0 until maxBuffers) {
    buffers(i) = 0L
    bufferSizes(i) = 0L
  }
  var currSlot : Int = 0
  var nLoaded : Int = -1

  /*
   * devicePointerSize is either 4 or 8 for the pointer in DenseVector + 4 for
   * size field + 4 for tiling field
   */
  val denseVectorStructSize = devicePointerSize + 4 + 4
  val outArgLength = N * denseVectorStructSize
  val outArgBuffer : Long = OpenCLBridge.nativeMalloc(outArgLength)

  override def next() : DenseVector = {
    val arr : Array[Double] = OpenCLBridge.getDenseVectorValuesFromOutputBuffers(
            buffers, outArgBuffer, currSlot, denseVectorStructSize, 0,
            devicePointerSize, devicePointerSize, devicePointerSize + 4)
    currSlot += 1
    Vectors.dense(arr).asInstanceOf[DenseVector]
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def kernelAttemptCallback(nLoaded : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int, heapTop : Int) {
    assert(devicePointerSize == 4 || devicePointerSize == 8)

    val nHeapBytesAvailable = if (heapTop > heapSize) heapSize else heapTop

    var heapOutBuffer : Long = 0L
    if (buffers(nFilledBuffers) != 0L) {
      if (nHeapBytesAvailable > bufferSizes(nFilledBuffers)) {
        buffers(nFilledBuffers) = OpenCLBridge.nativeRealloc(
                    buffers(nFilledBuffers), nHeapBytesAvailable)
        bufferSizes(nFilledBuffers) = nHeapBytesAvailable
      }
      heapOutBuffer = buffers(nFilledBuffers)
    } else {
      heapOutBuffer = OpenCLBridge.nativeMalloc(nHeapBytesAvailable)
      buffers(nFilledBuffers) = heapOutBuffer
      bufferSizes(nFilledBuffers) = nHeapBytesAvailable
    }
    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, heapArgStart,
            heapOutBuffer, nHeapBytesAvailable)
    nFilledBuffers += 1
  }

  override def finish(ctx : Long, dev_ctx : Long, outArgNum : Int, setNLoaded : Int) {
    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, outArgNum,
            outArgBuffer, outArgLength)
    nLoaded = setNLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def reset() {
    nFilledBuffers = 0
    currSlot = 0
    nLoaded = -1
  }

  override def releaseNativeArrays() {
    var iter : Int = 0
    while (iter < maxBuffers && buffers(iter) != 0L) {
      val buffer : Long = buffers(iter)
      OpenCLBridge.nativeFree(buffer)
      iter = iter + 1
    }
  }
}
