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

class DenseVectorDeviceBuffersWrapper(N : Int, anyFailedArgNum : Int,
    processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
    heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) {
  assert(devicePointerSize == 4 || devicePointerSize == 8)

  val heapTopArgnum : Int = processingSucceededArgnum - 2
  val anyFailed : Array[Int] = new Array[Int](1)
  val heapTop : Array[Int] = new Array[Int](1)
  val processingSucceeded : Array[Int] = new Array[Int](N)

  /*
   * devicePointerSize is either 4 or 8 for the pointer in DenseVector + 4 for
   * size field + 4 for tiling field
   */
  val denseVectorStructSize = devicePointerSize + 4 + 4
  val outArgLength = N * denseVectorStructSize
  val outArgBuffer : Long = OpenCLBridge.nativeMalloc(outArgLength)

  var heapOutBufferSize : Int = 0
  var heapOutBuffer : Long = 0

  def readFromDevice() : Boolean = {
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, anyFailedArgNum, anyFailed, 1)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, heapTopArgnum, heapTop, 1)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, N)

    val nHeapBytesAvailable = if (heapTop(0) > heapSize) heapSize else heapTop(0)
    if (heapOutBufferSize < nHeapBytesAvailable) {
      heapOutBufferSize = nHeapBytesAvailable
      heapOutBuffer = OpenCLBridge.nativeRealloc(heapOutBuffer, heapOutBufferSize)
    }

    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, outArgNum,
            outArgBuffer, outArgLength)
    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, heapArgStart,
            heapOutBuffer, nHeapBytesAvailable)

    anyFailed(0) == 0 // return true when the kernel completed successfully
  }

  def hasSlot(slot : Int) : Boolean = {
    processingSucceeded(slot) != 0
  }

  def get(slot : Int) : DenseVector = {
    val slotOffset = slot * denseVectorStructSize

    val values : Array[Double] =
        OpenCLBridge.deserializeChunkedValuesFromNativeArray(heapOutBuffer,
        outArgBuffer, slotOffset, slotOffset + devicePointerSize,
        devicePointerSize)

    Vectors.dense(values).asInstanceOf[DenseVector]
  }

  def releaseNativeArrays() {
    if (heapOutBufferSize > 0) {
      OpenCLBridge.nativeFree(heapOutBuffer)
    }
    OpenCLBridge.nativeFree(outArgBuffer)
  }
}

class DenseVectorOutputBufferWrapper(val N : Int)
    extends OutputBufferWrapper[DenseVector] {
  var nFilledBuffers : Int = 0
  val buffers : java.util.List[DenseVectorDeviceBuffersWrapper] =
      new java.util.LinkedList[DenseVectorDeviceBuffersWrapper]()
  var currSlot : Int = 0
  var nLoaded : Int = -1

  override def next() : DenseVector = {
    val iter = buffers.iterator
    var target : Option[DenseVectorDeviceBuffersWrapper] = None
    while (target.isEmpty && iter.hasNext) {
      val buffer = iter.next
      if (buffer.hasSlot(currSlot)) {
        target = Some(buffer)
      }
    }
    val vec = target.get.get(currSlot)
    currSlot += 1
    vec
  }

  override def hasNext() : Boolean = {
    currSlot < nLoaded
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) :
          Boolean = {
    var buffer : DenseVectorDeviceBuffersWrapper = null
    if (buffers.size > nFilledBuffers) {
      buffer = buffers.get(nFilledBuffers)
    } else {
      buffer = new DenseVectorDeviceBuffersWrapper(nLoaded, anyFailedArgNum,
                             processingSucceededArgnum, outArgNum, heapArgStart,
                             heapSize, ctx, dev_ctx, devicePointerSize)
      buffers.add(buffer)
    }
    nFilledBuffers += 1
    buffer.readFromDevice
  }

  override def finish(ctx : Long, dev_ctx : Long, outArgNum : Int, setNLoaded : Int) {
    nLoaded = setNLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def reset() {
    nFilledBuffers = 0
    currSlot = 0
    nLoaded = -1
  }

  override def releaseNativeArrays() {
    val iter : java.util.Iterator[DenseVectorDeviceBuffersWrapper] =
        buffers.iterator
    while (iter.hasNext) {
      val wrapper : DenseVectorDeviceBuffersWrapper = iter.next
      wrapper.releaseNativeArrays
    }
  }
}
