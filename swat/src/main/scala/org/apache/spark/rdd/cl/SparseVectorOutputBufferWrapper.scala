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

class SparseVectorDeviceBuffersWrapper(N : Int, anyFailedArgNum : Int,
        processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
        heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) {
  assert(devicePointerSize == 4 || devicePointerSize == 8)

  val heapTopArgnum : Int = processingSucceededArgnum - 2
  val anyFailed : Array[Int] = new Array[Int](1)
  val heapTop : Array[Int] = new Array[Int](1)
  val processingSucceeded : Array[Int] = new Array[Int](N)

  /*
   * devicePointerSize is either 4 or 8 for the pointers in SparseVector + 4 for
   * size field
   */
  val sparseVectorStructSize = (2 * devicePointerSize) + 4
  val outArgLength = N * sparseVectorStructSize
  val outArg : Array[Byte] = new Array[Byte](outArgLength)
  val outArgBuffer : ByteBuffer = ByteBuffer.wrap(outArg)
  outArgBuffer.order(ByteOrder.LITTLE_ENDIAN)

  var heapOutBufferSize : Int = 0
  var heapOutBuffer : Long = 0

  def readFromDevice() : Boolean = {
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, anyFailedArgNum, anyFailed, 1)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, heapTopArgnum, heapTop, 1)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, N)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, outArg, outArgLength)
    if (heapOutBufferSize < heapTop(0)) {
      heapOutBufferSize = heapTop(0)
      heapOutBuffer = OpenCLBridge.nativeRealloc(heapOutBuffer, heapOutBufferSize)
    }
    OpenCLBridge.fetchByteArrayArgToNativeArray(ctx, dev_ctx, heapArgStart,
            heapOutBuffer, heapTop(0))

    anyFailed(0) == 0 // return true when the kernel completed successfully
  }

  def hasSlot(slot : Int) : Boolean = {
    processingSucceeded(slot) != 0
  }

  def get(slot : Int) : SparseVector = {
    val slotOffset = slot * sparseVectorStructSize
    outArgBuffer.position(slotOffset)

    val indicesHeapOffset : Int = if (devicePointerSize == 4)
      outArgBuffer.getInt else outArgBuffer.getLong.toInt
    val valuesHeapOffset : Int = if (devicePointerSize == 4)
      outArgBuffer.getInt else outArgBuffer.getLong.toInt
    val size : Int = outArgBuffer.getInt

    val indices : Array[Int] =
        OpenCLBridge.deserializeChunkedIndicesFromNativeArray(heapOutBuffer,
        indicesHeapOffset, size)
    val values : Array[Double] =
        OpenCLBridge.deserializeChunkedValuesFromNativeArray(heapOutBuffer,
        valuesHeapOffset, size)

    Vectors.sparse(size, indices, values).asInstanceOf[SparseVector]
  }

  def releaseNativeArrays() {
    if (heapOutBufferSize > 0) {
      OpenCLBridge.nativeFree(heapOutBuffer)
    }
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

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) : Boolean = {
    var buffer : SparseVectorDeviceBuffersWrapper = null
    if (buffers.size > nFilledBuffers) {
      buffer = buffers.get(nFilledBuffers)
    } else {
      buffer = new SparseVectorDeviceBuffersWrapper(nLoaded, anyFailedArgNum,
                processingSucceededArgnum, outArgNum, heapArgStart, heapSize,
                ctx, dev_ctx, devicePointerSize)
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
    val iter : java.util.Iterator[SparseVectorDeviceBuffersWrapper] =
        buffers.iterator
    while (iter.hasNext) {
      val wrapper : SparseVectorDeviceBuffersWrapper = iter.next
      wrapper.releaseNativeArrays
    }
  }
}
