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

class DenseVectorOutputBufferWrapper(outArgNum : Int, heapArgStart : Int,
        heapSize : Int, ctx : Long, dev_ctx : Long, nLoaded : Int, anyFailedArgNum : Int, entryPoint : Entrypoint, bbCache : ByteBufferCache)
        extends OutputBufferWrapper[DenseVector] {
  val processingSucceededArgnum = heapArgStart + 3
  val processingSucceeded : Array[Int] = new Array[Int](nLoaded)

  // 8 for the pointer in DenseVector + 4 for size
  val denseVectorStructSize = 8 + 4
  val outArgLength = nLoaded * denseVectorStructSize
  val outArg : Array[Byte] = new Array[Byte](outArgLength)
  val outArgBuffer : ByteBuffer = ByteBuffer.wrap(outArg)
  outArgBuffer.order(ByteOrder.LITTLE_ENDIAN)

  val heapOut : Array[Byte] = new Array[Byte](heapSize)
  val heapOutBuffer : ByteBuffer = ByteBuffer.wrap(heapOut)
  heapOutBuffer.order(ByteOrder.LITTLE_ENDIAN)

  val anyFailed : Array[Int] = new Array[Int](1)

  var complete = false

  var iter : Int = nLoaded

  def runKernelOnce() {
    OpenCLBridge.run(ctx, dev_ctx, nLoaded);
    OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, anyFailedArgNum,
            anyFailed, entryPoint, bbCache)
    OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, processingSucceededArgnum,
            processingSucceeded, nLoaded)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, outArg, outArgLength)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, heapArgStart, heapOut,
            heapSize)
  }

  def moveToNextNonEmptySlot() : Boolean = {
    while (iter < nLoaded && processingSucceeded(iter) == 0) {
      iter += 1
    }
    iter != nLoaded
  }

  override def next() : DenseVector = {
    if (!moveToNextNonEmptySlot) {
      // Reset and run again
      runKernelOnce
      complete = (anyFailed(0) == 0)
      iter = 0
      val success = moveToNextNonEmptySlot
      assert(success)
    }

    val slot = iter
    val slotOffset = slot * denseVectorStructSize
    outArgBuffer.position(slotOffset)
    val heapOffset : Long = outArgBuffer.getLong
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

  override def hasNext() : Boolean = {
    complete && iter == nLoaded
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    OpenCLBridge.postKernelCleanup(ctx);
  }
}
