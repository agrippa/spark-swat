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

class SparseVectorDeviceBuffersWrapper(N : Int,
        processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
        heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) {
  assert(devicePointerSize == 4 || devicePointerSize == 8)

  val processingSucceeded : Array[Int] = new Array[Int](N)

  /*
   * devicePointerSize is either 4 or 8 for the pointers in SparseVector + 4 for
   * size field
   */
  val sparseVectorStructSize = (2 * devicePointerSize) + 4 + 4
  val outArgLength = N * sparseVectorStructSize
  val outArgBuffer : Long = OpenCLBridge.nativeMalloc(outArgLength)

  var heapOutBufferSize : Int = 0
  var heapOutBuffer : Long = 0

  def readFromDevice(heapTop : Int) {
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, N)

    if (heapOutBufferSize < heapTop) {
      heapOutBufferSize = heapTop
      heapOutBuffer = OpenCLBridge.nativeRealloc(heapOutBuffer, heapOutBufferSize)
    }

    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, outArgNum,
            outArgBuffer, outArgLength)
    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, heapArgStart,
            heapOutBuffer, heapTop)
  }

  def hasSlot(slot : Int) : Boolean = {
    processingSucceeded(slot) != 0
  }

  def get(slot : Int) : SparseVector = {
    val slotOffset = slot * sparseVectorStructSize

    val indices : Array[Int] =
        OpenCLBridge.deserializeChunkedIndicesFromNativeArray(heapOutBuffer,
        outArgBuffer, slotOffset, slotOffset + 2 * devicePointerSize, devicePointerSize)
    val values : Array[Double] =
        OpenCLBridge.deserializeChunkedValuesFromNativeArray(heapOutBuffer,
        outArgBuffer, slotOffset + devicePointerSize,
        slotOffset + 2 * devicePointerSize, devicePointerSize)
    assert(indices.size == values.size)

    Vectors.sparse(indices.size, indices, values).asInstanceOf[SparseVector]
  }

  def releaseNativeArrays() {
    if (heapOutBufferSize > 0) {
      OpenCLBridge.nativeFree(heapOutBuffer)
    }
    OpenCLBridge.nativeFree(outArgBuffer)
  }
}

class SparseVectorOutputBufferWrapper(val N : Int)
    extends OutputBufferWrapper[SparseVector] {
  var nFilledBuffers : Int = 0
  val buffers : java.util.List[SparseVectorDeviceBuffersWrapper] =
      new java.util.LinkedList[SparseVectorDeviceBuffersWrapper]()
  var currSlot : Int = 0
  var nLoaded : Int = -1

  override def next() : SparseVector = {
    val iter = buffers.iterator
    var target : Option[SparseVectorDeviceBuffersWrapper] = None
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

  override def kernelAttemptCallback(nLoaded : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int, heapTop : Int) {
    var buffer : SparseVectorDeviceBuffersWrapper = null
    if (buffers.size > nFilledBuffers) {
      buffer = buffers.get(nFilledBuffers)
    } else {
      buffer = new SparseVectorDeviceBuffersWrapper(nLoaded,
                processingSucceededArgnum, outArgNum, heapArgStart, heapSize,
                ctx, dev_ctx, devicePointerSize)
      buffers.add(buffer)
    }
    nFilledBuffers += 1
    buffer.readFromDevice(heapTop)
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
    val iter : java.util.Iterator[SparseVectorDeviceBuffersWrapper] =
        buffers.iterator
    while (iter.hasNext) {
      val wrapper : SparseVectorDeviceBuffersWrapper = iter.next
      wrapper.releaseNativeArrays
    }
  }
}
