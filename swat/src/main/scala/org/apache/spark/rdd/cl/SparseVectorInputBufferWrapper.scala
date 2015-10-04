package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder
import java.nio.IntBuffer
import java.nio.DoubleBuffer
import java.nio.ByteBuffer

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.UnparameterizedMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.writer.KernelWriter

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

object SparseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
}

class SparseVectorInputBufferWrapper (val vectorElementCapacity : Int,
        val vectorCapacity : Int, entryPoint : Entrypoint)
        extends InputBufferWrapper[SparseVector] {

  def this(vectorCapacity : Int, entryPoint : Entrypoint) =
        this(vectorCapacity * 30, vectorCapacity, entryPoint)

  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.SparseVector", new UnparameterizedMatcher())
  val sparseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0
  var iter : Int = 0

  val tiling : Int = SparseVectorInputBufferWrapperConfig.tiling
  var tiled : Int = 0
  val to_tile : Array[SparseVector] = new Array[SparseVector](tiling)
  val to_tile_sizes : Array[Int] = new Array[Int](tiling)

  val next_buffered_values : Array[Array[Double]] = new Array[Array[Double]](tiling)
  val next_buffered_indices : Array[Array[Int]] = new Array[Array[Int]](tiling)
  var next_buffered_iter : Int = 0
  var n_next_buffered : Int = 0

  var bufferPosition : Int = 0
  val valuesBuffer : Long = OpenCLBridge.nativeMalloc(vectorElementCapacity * 8)
  val indicesBuffer : Long = OpenCLBridge.nativeMalloc(vectorElementCapacity * 4)

  val sizesBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)
  val offsetsBuffer : Long = OpenCLBridge.nativeMalloc(vectorCapacity * 4)

  val overrun : Array[SparseVector] = new Array[SparseVector](tiling)

  override def flush() {
    if (tiled > 0) {
      val nTiled : Int = OpenCLBridge.serializeStridedSparseVectorsToNativeBuffer(
          valuesBuffer, indicesBuffer, bufferPosition, vectorElementCapacity, sizesBuffer,
          offsetsBuffer, buffered, vectorCapacity, to_tile, to_tile_sizes,
          if (buffered + tiled > vectorCapacity) (vectorCapacity - buffered) else tiled, tiling)
      if (nTiled > 0) {
        var newBufferPosition : Int = bufferPosition + 0 +
            (tiling * (to_tile(0).size - 1))

        for (i <- 1 until nTiled) {
          val curr : SparseVector = to_tile(i)
          var pos : Int = bufferPosition + i + (tiling * (curr.size - 1))
          if (pos > newBufferPosition) {
            newBufferPosition = pos
          }
        }

        bufferPosition = newBufferPosition + 1
      }

      val nFailed = tiled - nTiled
      if (nFailed > 0) {
        for (i <- nTiled until tiled) {
          overrun(i - nTiled) = to_tile(i)
        }
      }

      buffered += nTiled
      tiled = 0
    }
  }

  override def append(obj : Any) {
    append(obj.asInstanceOf[SparseVector])
  }

  def append(obj : SparseVector) {
    to_tile(tiled) = obj
    to_tile_sizes(tiled) = obj.size
    tiled += 1

    if (tiled == tiling) {
        flush
    }
  }

  override def aggregateFrom(iterator : Iterator[SparseVector]) {
    assert(overrun(0) == null)
    while (iterator.hasNext && overrun(0) == null) {
      val next : SparseVector = iterator.next
      append(next)
    }
  }

  override def nBuffered() : Int = {
    if (tiled > 0) {
      flush
    }
    buffered
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
          cacheID : CLCacheID) : Int = {
    assert(tiled == 0)

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, sparseVectorStructSize * vectorCapacity)
    // indices array, size of double = 4
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 1,
            indicesBuffer, bufferPosition * 4, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
    // values array, size of double = 8
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 2,
            valuesBuffer, bufferPosition * 8, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component + 1)
    // Sizes of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 3, sizesBuffer,
            buffered * 4, cacheID.broadcast, cacheID.rdd, cacheID.partition,
            cacheID.offset, cacheID.component + 2)
    // Offsets of each vector
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum + 4, offsetsBuffer,
            buffered * 4, cacheID.broadcast, cacheID.rdd, cacheID.partition,
            cacheID.offset, cacheID.component + 3)
    // Number of sparse vectors being copied
    OpenCLBridge.setIntArg(ctx, argnum + 5, buffered)

    return 6
  }

  override def hasNext() : Boolean = {
    iter < buffered
  }

  override def next() : SparseVector = {
    assert(tiled == 0)
    if (next_buffered_iter == n_next_buffered) {
        next_buffered_iter = 0
        n_next_buffered = if (buffered - iter > tiling) tiling else buffered - iter
        OpenCLBridge.deserializeStridedValuesFromNativeArray(
                next_buffered_values.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, valuesBuffer, sizesBuffer, offsetsBuffer, iter,
                tiling)
        OpenCLBridge.deserializeStridedIndicesFromNativeArray(
                next_buffered_indices.asInstanceOf[Array[java.lang.Object]],
                n_next_buffered, indicesBuffer, sizesBuffer, offsetsBuffer, iter,
                tiling)
    }

    val values : Array[Double] = next_buffered_values(next_buffered_iter)
    val indices : Array[Int] = next_buffered_indices(next_buffered_iter)
    val result : SparseVector = Vectors.sparse(indices.size, indices, values)
            .asInstanceOf[SparseVector]
    next_buffered_iter += 1
    iter += 1
    result
  }

  override def haveUnprocessedInputs : Boolean = {
    overrun(0) != null
  }

  override def outOfSpace : Boolean = {
    overrun(0) != null
  }

  override def releaseNativeArrays {
    OpenCLBridge.nativeFree(valuesBuffer)
    OpenCLBridge.nativeFree(indicesBuffer)
    OpenCLBridge.nativeFree(sizesBuffer)
    OpenCLBridge.nativeFree(offsetsBuffer)
  }

  override def reset() {
    buffered = 0
    iter = 0
    bufferPosition = 0
    var i = 0
    while (i < tiling && overrun(i) != null) {
      // TODO what if we run out of space while handling the overrun...
      append(overrun(i))
      overrun(i) = null
      i += 1
    }
  }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long, entrypoint : Entrypoint) :
      Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0 + 1, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 4)) {
      val nVectors : Int = OpenCLBridge.fetchNLoaded(id.rdd, id.partition, id.offset)
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.SPARSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.SPARSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, 0,
              c.getTotalStructSize * nVectors)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, 0 + 5, nVectors)
      return 6
    } else {
      return -1
    }
  }
}
