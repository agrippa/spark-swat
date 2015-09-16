package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder
import java.nio.DoubleBuffer
import java.nio.ByteBuffer

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.UnparameterizedMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.InterruptibleIterator

object DenseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
}

class DenseVectorInputBufferWrapper(val vectorElementCapacity : Int, val vectorCapacity : Int,
        entryPoint : Entrypoint) extends InputBufferWrapper[DenseVector] {

  def this(vectorCapacity : Int, entryPoint : Entrypoint) =
      this(vectorCapacity * 70, vectorCapacity, entryPoint)

  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.DenseVector", new UnparameterizedMatcher())
  val denseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0
  var iter : Int = 0

  val tiling : Int = DenseVectorInputBufferWrapperConfig.tiling
  var tiled : Int = 0
  val to_tile : Array[DenseVector] = new Array[DenseVector](tiling)

  val valuesBuffer : Long = OpenCLBridge.nativeMalloc(vectorElementCapacity * 8)
  var valuesBufferPosition : Int = 0
  val valuesBufferCapacity : Long = vectorElementCapacity * 8

  val sizes : Array[Int] = new Array[Int](vectorCapacity)
  val offsets : Array[Int] = new Array[Int](vectorCapacity)

  val overrun : Array[DenseVector] = new Array[DenseVector](tiling)

  override def flush() {
    val nTiled : Int = OpenCLBridge.serializeStridedDenseVectorsToNativeBuffer(
        valuesBuffer, valuesBufferPosition, valuesBufferCapacity, to_tile,
        if (buffered + tiled > vectorCapacity) (vectorCapacity - buffered) else tiled, tiling)
    if (nTiled > 0) {
      var newValuesBufferPosition : Int = valuesBufferPosition + 0 +
          (tiling * (to_tile(0).size - 1))
      sizes(buffered) = to_tile(0).size
      offsets(buffered) = valuesBufferPosition

      for (i <- 1 until nTiled) {
        val curr : DenseVector = to_tile(i)
        var pos : Int = valuesBufferPosition + i + (tiling * (curr.size - 1))
        if (pos > newValuesBufferPosition) {
          newValuesBufferPosition = pos
        }

        sizes(buffered + i) = curr.size
        offsets(buffered + i) = valuesBufferPosition + i
      }

      valuesBufferPosition = newValuesBufferPosition + 1
    }

    val nFailed = tiled - nTiled
    if (nFailed > 0) {
      for (i <- nTiled until tiled) {
        overrun(i - nTiled) = to_tile(i)
      }
      System.err.println("nFailed=" + nFailed)
    }

    buffered += nTiled
    tiled = 0
  }

  override def append(obj : Any) {
    append(obj.asInstanceOf[DenseVector])
  }

  def append(obj : DenseVector) {
    to_tile(tiled) = obj
    tiled += 1

    if (tiled == tiling) {
        flush
    }
  }

  override def aggregateFrom(iter : Iterator[DenseVector]) : Int = {
    assert(overrun(0) == null)
    while (iter.hasNext && overrun(0) == null) {
      val next : DenseVector = iter.next
      append(next)
    }
    buffered + tiled
  }

  override def nBuffered() : Int = {
    buffered
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID) : Int = {
    if (tiled > 0) {
      flush
    }

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
            denseVectorStructSize * vectorCapacity)
    // values array, size of double = 8
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 1, valuesBuffer,
        valuesBufferPosition, cacheID.broadcast, cacheID.rdd, cacheID.partition,
        cacheID.offset, cacheID.component)
    // Sizes of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 2, sizes, buffered,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 1)
    // Offsets of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 3, offsets, buffered,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 2)
    // Number of vectors
    OpenCLBridge.setIntArg(ctx, argnum + 4, buffered)

    buffered = 0
    valuesBufferPosition = 0

    if (overrun(0) != null) {
      var i = 0
      while (overrun(i) != null) {
        // TODO what if we run out of space while handling the overrun...
        append(overrun(i))
        overrun(i) = null
        i += 1
      }
    }

    return 5
  }

  override def hasNext() : Boolean = {
    iter < buffered
  }

  override def next() : DenseVector = {
    if (tiled > 0) { // Flush once, on first call to next
      flush
    }
    val vectorSize : Int = sizes(iter)
    val vectorOffset : Int = offsets(iter)
    val vectorArr : Array[Double] = new Array[Double](vectorSize)
    OpenCLBridge.fillFromNativeArray(vectorArr, vectorSize, vectorOffset,
        tiling, valuesBuffer)
    Vectors.dense(vectorArr).asInstanceOf[DenseVector]
  }

  override def haveUnprocessedInputs : Boolean = {
    // True if overrun was non-empty after copying to device
    assert(overrun(0) == null)
    buffered + tiled > 0
  }

  override def releaseNativeArrays {
    OpenCLBridge.nativeFree(valuesBuffer)
  }
}
