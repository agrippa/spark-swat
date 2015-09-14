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

class DenseVectorDeviceBuffersWrapper(nLoaded : Int, anyFailedArgNum : Int,
        processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
        heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
        bbCache : ByteBufferCache, devicePointerSize : Int) {
  val anyFailed : Array[Int] = new Array[Int](1)

  val processingSucceeded : Array[Int] = new Array[Int](nLoaded)

  /*
   * devicePointerSize is either 4 or 8 for the pointer in DenseVector + 4 for
   * size field
   */
  val denseVectorStructSize = devicePointerSize + 4
  val outArgLength = nLoaded * denseVectorStructSize
  val outArg : Array[Byte] = new Array[Byte](outArgLength)
  val outArgBuffer : ByteBuffer = ByteBuffer.wrap(outArg)
  outArgBuffer.order(ByteOrder.LITTLE_ENDIAN)

  val heapOut : Array[Byte] = new Array[Byte](heapSize)
  val heapOutBuffer : ByteBuffer = ByteBuffer.wrap(heapOut)
  heapOutBuffer.order(ByteOrder.LITTLE_ENDIAN)

  def readFromDevice() : Boolean = {
    OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, anyFailedArgNum,
            anyFailed, entryPoint, bbCache)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, nLoaded)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, outArg, outArgLength)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, heapArgStart, heapOut,
            heapSize)

    anyFailed(0) == 0 // return true when the kernel completed successfully
  }

  def hasSlot(slot : Int) : Boolean = {
    processingSucceeded(slot) != 0
  }

  def get(slot : Int) : DenseVector = {
    val slotOffset = slot * denseVectorStructSize
    outArgBuffer.position(slotOffset)
    var heapOffset : Int = -1
    if (devicePointerSize == 4) {
      heapOffset = outArgBuffer.getInt
    } else if (devicePointerSize == 8) {
      heapOffset = outArgBuffer.getLong.toInt
    } else {
      throw new RuntimeException("Unsupported devicePointerSize=" + devicePointerSize)
    }
    val size : Int = outArgBuffer.getInt

    val values : Array[Double] = new Array[Double](size)
    heapOutBuffer.position(heapOffset.toInt)
    var i = 0
    while (i < size) {
      values(i) = heapOutBuffer.getDouble
      i += 1
    }

    Vectors.dense(values).asInstanceOf[DenseVector]
  }
}

class DenseVectorOutputBufferWrapper(val nLoaded : Int, val outArgNum : Int)
    extends OutputBufferWrapper[DenseVector] {
  val buffers : java.util.List[DenseVectorDeviceBuffersWrapper] =
      new java.util.LinkedList[DenseVectorDeviceBuffersWrapper]()
  var currSlot = 0

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
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
    val buffer : DenseVectorDeviceBuffersWrapper =
        new DenseVectorDeviceBuffersWrapper(nLoaded, anyFailedArgNum,
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
