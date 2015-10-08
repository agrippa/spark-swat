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
import com.amd.aparapi.internal.writer.KernelWriter

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.InterruptibleIterator

object DenseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
  val avgVecLength_str = System.getProperty("swat.avg_vec_length")
  val avgVecLength = if (avgVecLength_str == null) 70 else avgVecLength_str.toInt
}

class DenseVectorInputBufferWrapper(val vectorElementCapacity : Int,
        val vectorCapacity : Int, val tiling : Int, entryPoint : Entrypoint,
        val blockingCopies : Boolean) extends InputBufferWrapper[DenseVector] {

  def this(vectorCapacity : Int, tiling : Int, entryPoint : Entrypoint,
          blockingCopies : Boolean) = this(
              vectorCapacity * DenseVectorInputBufferWrapperConfig.avgVecLength,
              vectorCapacity, tiling, entryPoint, blockingCopies)

  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.DenseVector", new UnparameterizedMatcher())
  val denseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0
  var iter : Int = 0
  var vectorsUsed : Int = -1
  var elementsUsed : Int = -1

  var tiled : Int = 0
  val to_tile : Array[DenseVector] = new Array[DenseVector](tiling)
  val to_tile_sizes : Array[Int] = new Array[Int](tiling)

  val next_buffered : Array[Array[Double]] = new Array[Array[Double]](tiling)
  var next_buffered_iter : Int = 0
  var n_next_buffered : Int = 0

  val valuesBuffer : Long = OpenCLBridge.nativeMalloc(vectorElementCapacity * 8)
  var valuesBufferPosition : Int = 0

  val sizesBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)

  val overrun : Array[DenseVector] = new Array[DenseVector](tiling)
  var haveOverrun : Boolean = false

  override def flush() {
    if (tiled > 0) {
      val nTiled : Int = OpenCLBridge.serializeStridedDenseVectorsToNativeBuffer(
          valuesBuffer, valuesBufferPosition, vectorElementCapacity, sizesBuffer,
          offsetsBuffer, buffered, vectorCapacity, to_tile, to_tile_sizes,
          if (buffered + tiled > vectorCapacity) (vectorCapacity - buffered) else tiled, tiling)
      if (nTiled > 0) {
        var newValuesBufferPosition : Int = valuesBufferPosition + 0 +
            (tiling * (to_tile(0).size - 1))

        for (i <- 1 until nTiled) {
          val curr : DenseVector = to_tile(i)
          var pos : Int = valuesBufferPosition + i + (tiling * (curr.size - 1))
          if (pos > newValuesBufferPosition) {
            newValuesBufferPosition = pos
          }
        }

        valuesBufferPosition = newValuesBufferPosition + 1
      }

      val nFailed = tiled - nTiled
      if (nFailed > 0) {
        for (i <- nTiled until tiled) {
          overrun(i - nTiled) = to_tile(i)
        }
        haveOverrun = true
      }

      buffered += nTiled
      tiled = 0
    }
  }

  override def append(obj : Any) {
    append(obj.asInstanceOf[DenseVector])
  }

  def append(obj : DenseVector) {
    to_tile(tiled) = obj
    to_tile_sizes(tiled) = obj.size
    tiled += 1

    if (tiled == tiling) {
        flush
    }
  }

  override def aggregateFrom(iterator : Iterator[DenseVector]) {
    assert(!haveOverrun)
    while (iterator.hasNext && !haveOverrun) {
      append(iterator.next)
    }
  }

  override def nBuffered() : Int = {
    if (tiled > 0) {
      flush
    }
    buffered
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean, limit : Int = -1) : Int = {
    // Should call a flush explicitly from the RDD iterator next() function
    assert(tiled == 0)
    val vectorsToCopy = if (limit == -1) buffered else limit
    assert(vectorsToCopy <= buffered)
    val elementsToCopy = if (vectorsToCopy == buffered) valuesBufferPosition else
        OpenCLBridge.getMaxOffsetOfStridedVectors(vectorsToCopy, sizesBuffer,
                offsetsBuffer, tiling) + 1

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
            denseVectorStructSize * vectorCapacity, persistent)
    // values array, size of double = 8
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 1, valuesBuffer,
        elementsToCopy * 8, cacheID.broadcast, cacheID.rdd, cacheID.partition,
        cacheID.offset, cacheID.component, persistent, blockingCopies)
    // Sizes of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 2, sizesBuffer, vectorsToCopy * 4,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 1, persistent, blockingCopies)
    // Offsets of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 3, offsetsBuffer, vectorsToCopy * 4,
            cacheID.broadcast, cacheID.rdd, cacheID.partition, cacheID.offset,
            cacheID.component + 2, persistent, blockingCopies)
    // Number of vectors
    OpenCLBridge.setIntArg(ctx, argnum + 4, vectorsToCopy)
    // Tiling
    OpenCLBridge.setIntArg(ctx, argnum + 5, tiling)

    vectorsUsed = vectorsToCopy
    elementsUsed = elementsToCopy

    return countArgumentsUsed
  }

  override def countArgumentsUsed : Int = { 6 }

  override def hasNext() : Boolean = {
    iter < buffered
  }

  override def next() : DenseVector = {
    assert(tiled == 0)
    if (next_buffered_iter == n_next_buffered) {
        next_buffered_iter = 0
        n_next_buffered = if (buffered - iter > tiling) tiling else buffered - iter
        OpenCLBridge.deserializeStridedValuesFromNativeArray(
                next_buffered.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer, iter, tiling)
    }
    val result : DenseVector = Vectors.dense(next_buffered(next_buffered_iter))
        .asInstanceOf[DenseVector]
    next_buffered_iter += 1
    iter += 1
    result
  }

  /*
   * True if overrun was non-empty after copying to device and we transferred
   * some vectors from overrun into to_tile.
   */
  override def haveUnprocessedInputs : Boolean = {
    haveOverrun
  }

  override def outOfSpace : Boolean = {
    haveOverrun
  }

  override def releaseNativeArrays {
    OpenCLBridge.nativeFree(valuesBuffer)
    OpenCLBridge.nativeFree(sizesBuffer)
    OpenCLBridge.nativeFree(offsetsBuffer)
  }

  override def reset() {
    if (vectorsUsed != -1 && vectorsUsed != buffered) {
      assert(elementsUsed != -1 && elementsUsed != valuesBufferPosition)

      val leftoverVectors = buffered - vectorsUsed
      val leftoverElements = valuesBufferPosition - elementsUsed
      OpenCLBridge.resetDenseVectorBuffers(valuesBuffer, sizesBuffer,
              offsetsBuffer, vectorsUsed, elementsUsed, leftoverVectors,
              leftoverElements)
      buffered = leftoverVectors
      valuesBufferPosition = leftoverElements
    } else {
      buffered = 0
      valuesBufferPosition = 0
    }

    vectorsUsed = -1
    elementsUsed = -1

    iter = 0
    haveOverrun = false
    var i = 0
    while (i < tiling && overrun(i) != null) {
      // TODO what if we run out of space while handling the overrun...
      append(overrun(i))
      overrun(i) = null
      i += 1
    }
  }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entrypoint : Entrypoint, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0 + 1, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 3, persistent)) {
      val nVectors : Int = OpenCLBridge.fetchNLoaded(id.rdd, id.partition, id.offset)
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.DENSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.DENSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, 0,
              c.getTotalStructSize * nVectors, persistent)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, 0 + 4, nVectors)
      // Tiling
      OpenCLBridge.setIntArg(ctx, 0 + 5, tiling)

      return countArgumentsUsed
    } else {
      return -1
    }
  }
}
