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

class SparseVectorDeviceBuffersWrapper(nLoaded : Int, anyFailedArgNum : Int,
        processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
        heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
        bbCache : ByteBufferCache, devicePointerSize : Int) {
  val anyFailed : Array[Int] = new Array[Int](1)

  val processingSucceeded : Array[Int] = new Array[Int](nLoaded)

  /*
   * devicePointerSize is either 4 or 8 for the pointers in SparseVector + 4 for
   * size field
   */
  val sparseVectorStructSize = (2 * devicePointerSize) + 4
  val outArgLength = nLoaded * sparseVectorStructSize
  val outArg : Array[Byte] = new Array[Byte](outArgLength)
  val outArgBuffer : ByteBuffer = ByteBuffer.wrap(outArg)
  outArgBuffer.order(ByteOrder.LITTLE_ENDIAN)

  val heapOut : Array[Byte] = new Array[Byte](heapSize)
  val heapOutBuffer : ByteBuffer = ByteBuffer.wrap(heapOut)
  heapOutBuffer.order(ByteOrder.LITTLE_ENDIAN)

  var iter : Int = 0

  def readFromDevice() : Boolean = {
    OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, anyFailedArgNum,
            anyFailed, entryPoint, bbCache)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, nLoaded)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, outArg, outArgLength)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, heapArgStart, heapOut,
            heapSize)

    if (processingSucceeded(iter) == 0) {
      // If we aren't already on a non-empty, move
      moveToNextNonEmptySlot
    }
    assert(hasNext) // assume that each buffer has at least one item

    anyFailed(0) == 0 // return true when the kernel completed successfully
  }

  def moveToNextNonEmptySlot() : Boolean = {
    iter += 1
    while (iter < nLoaded && processingSucceeded(iter) == 0) {
      iter += 1
    }
    hasNext
  }

  def getCurrSlot() : Int = {
    iter
  }

  def hasNext() : Boolean = {
    iter != nLoaded
  }

  def next() : SparseVector = {
    val slot = iter
    val slotOffset = slot * sparseVectorStructSize
    outArgBuffer.position(slotOffset)
    var indicesHeapOffset : Int = -1
    var valuesHeapOffset : Int = -1
    if (devicePointerSize == 4) {
      indicesHeapOffset = outArgBuffer.getInt
      valuesHeapOffset = outArgBuffer.getInt
    } else if (devicePointerSize == 8) {
      indicesHeapOffset = outArgBuffer.getLong.toInt
      valuesHeapOffset = outArgBuffer.getLong.toInt
    } else {
      throw new RuntimeException("Unsupported devicePointerSize=" + devicePointerSize)
    }
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

class SparseVectorOutputBufferWrapper(val outArgNum : Int)
    extends OutputBufferWrapper[SparseVector] {
  val buffers : java.util.List[SparseVectorDeviceBuffersWrapper] =
      new java.util.LinkedList[SparseVectorDeviceBuffersWrapper]()
  var completed = 0
  var currSlot = 0

  override def next() : SparseVector = {
    val iter = buffers.iterator
    var target : Option[SparseVectorDeviceBuffersWrapper] = None
    while (target.isEmpty && iter.hasNext) {
      val buffer = iter.next
      if (buffer.getCurrSlot == currSlot) {
        target = Some(buffer)
      }
    }
    val vec = target.get.next
    if (!target.get.moveToNextNonEmptySlot) {
      completed += 1
    }
    currSlot += 1
    vec
  }

  override def hasNext() : Boolean = {
    completed != buffers.size
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
    val buffer : SparseVectorDeviceBuffersWrapper =
        new SparseVectorDeviceBuffersWrapper(nLoaded, anyFailedArgNum,
                processingSucceededArgnum, outArgNum, heapArgStart, heapSize,
                ctx, dev_ctx, entryPoint, bbCache, devicePointerSize)
    val complete : Boolean = buffer.readFromDevice
    buffers.add(buffer)
    complete
  }

  override def finish(ctx : Long, dev_ctx : Long) { }

  override def releaseBuffers(bbCache : ByteBufferCache) { }

  override def countArgumentsUsed() : Int = { 1 }
}
