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

  val anyFailed : Array[Int] = new Array[Int](1)
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

  val heapOut : Array[Byte] = new Array[Byte](heapSize)
  val heapOutBuffer : ByteBuffer = ByteBuffer.wrap(heapOut)
  heapOutBuffer.order(ByteOrder.LITTLE_ENDIAN)

  def readFromDevice() : Boolean = {
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, anyFailedArgNum, anyFailed, 1)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, N)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, outArg, outArgLength)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, heapArgStart, heapOut,
            heapSize)

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

    val indices : Array[Int] = new Array[Int](size)
    val values : Array[Double] = new Array[Double](size)

    // Pull out indices
    heapOutBuffer.position(indicesHeapOffset)
    var i = 0
    while (i < size) {
      indices(i) = heapOutBuffer.getInt
      i += 1
    }
    // Pull out values
    heapOutBuffer.position(valuesHeapOffset)
    i = 0
    while (i < size) {
      values(i) = heapOutBuffer.getDouble
      i += 1
    }

    Vectors.sparse(size, indices, values).asInstanceOf[SparseVector]
  }
}

class SparseVectorOutputBufferWrapper(val N : Int)
    extends OutputBufferWrapper[SparseVector] {
  var nFilledBuffers : Int = 0
  val buffers : java.util.List[SparseVectorDeviceBuffersWrapper] =
      new java.util.LinkedList[SparseVectorDeviceBuffersWrapper]()
  var currSlot : Int = 0
  var nLoaded : Int = 0

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
}
