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
  val structSize = classModel.getTotalStructSize

  var buffered : Int = 0
  var iter : Int = 0

  val tiling : Int = SparseVectorInputBufferWrapperConfig.tiling
  var tiled : Int = 0
  val to_tile : Array[SparseVector] = new Array[SparseVector](tiling)

  val valuesBB : ByteBuffer = ByteBuffer.allocate(vectorElementCapacity * 8)
  valuesBB.order(ByteOrder.LITTLE_ENDIAN)
  val doubleValuesBB : DoubleBuffer = valuesBB.asDoubleBuffer
  val indicesBB : ByteBuffer = ByteBuffer.allocate(vectorElementCapacity * 4)
  indicesBB.order(ByteOrder.LITTLE_ENDIAN)
  val intIndicesBB : IntBuffer = indicesBB.asIntBuffer

  var currentTileOffset : Int = 0

  val sizes : Array[Int] = new Array[Int](vectorCapacity)
  val offsets : Array[Int] = new Array[Int](vectorCapacity)

  var overrun : Option[SparseVector] = None

  def calcTileEleStartingOffset(ele : Int) : Int = {
    currentTileOffset + ele
  }

  // inclusive
  def calcTileEleEndingOffsetHelper(ele : Int, eleSize : Int) : Int = {
    calcTileEleStartingOffset(ele) + (tiling * (eleSize - 1))
  }
  def calcTileEleEndingOffset(ele : Int) : Int = {
    calcTileEleEndingOffsetHelper(ele, to_tile(ele).size)
  }

  /*
   * Given the next vector we want to add to the input buffer, will we run out
   * of space?
   */
  def willRunOutOfSpace(next : SparseVector) : Boolean = {
    assert(tiled < tiling)
    if (buffered + tiled == vectorCapacity) {
      return true
    } else if (calcTileEleEndingOffsetHelper(tiled, next.size) >= vectorElementCapacity) {
      return true
    } else {
      return false
    }
  }

  override def flush() {
      var maximumOffsetUsed = 0
      for (i <- 0 until tiled) {
        val curr : SparseVector = to_tile(i)

        val startingOffset = calcTileEleStartingOffset(i)
        val endingOffset = calcTileEleEndingOffset(i)

        var currOffset = startingOffset
        for (j <- 0 until curr.size) {
          doubleValuesBB.put(currOffset, curr.values(j))
          intIndicesBB.put(currOffset, curr.indices(j))
          currOffset += tiling
        }

        sizes(buffered + i) = curr.size
        offsets(buffered + i) = startingOffset
        if (endingOffset > maximumOffsetUsed) {
          maximumOffsetUsed = endingOffset
        }
      }

      buffered += tiled
      tiled = 0
      currentTileOffset = maximumOffsetUsed + 1
  }

  override def append(obj : Any) {
    append(obj.asInstanceOf[SparseVector])
  }

  def append(obj : SparseVector) {
    if (tiled == tiling) {
        flush
    }

    if (willRunOutOfSpace(obj)) {
      // Assert not just one vector consuming all buffer space
      assert(buffered + tiled > 0)
      overrun = Some(obj)
    } else {
      to_tile(tiled) = obj
      tiled += 1
    }
  }

  override def aggregateFrom(iter : Iterator[SparseVector]) {
    assert(overrun.isEmpty)
    while (iter.hasNext && overrun.isEmpty) {
      val next : SparseVector = iter.next
      append(next)
    }
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
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, structSize * vectorCapacity)
    // indices array, size of double = 4
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 1,
            indicesBB.array, currentTileOffset, 4, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
    // values array, size of double = 8
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 2,
            valuesBB.array, currentTileOffset, 8, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component + 1)
    // Sizes of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 3, sizes, buffered, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component + 2)
    // Offsets of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 4, offsets, buffered, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component + 3)
    // Number of sparse vectors being copied
    OpenCLBridge.setIntArg(ctx, argnum + 5, buffered)

    buffered = 0
    currentTileOffset = 0

    if (!overrun.isEmpty) {
      append(overrun.get)
      overrun = None
    }

    return 6
  }

  override def hasNext() : Boolean = {
    iter < buffered
  }

  override def next() : SparseVector = {
    if (tiled > 0) {
      flush
    }
    val vectorSize : Int = sizes(iter)
    val vectorOffset : Int = offsets(iter)
    val vectorValues : Array[Double] = new Array[Double](vectorSize)
    val vectorIndices : Array[Int] = new Array[Int](vectorSize)
    var i = 0
    while (i < vectorSize) {
      vectorValues(i) = valuesBB.get(vectorOffset + i * tiling)
      vectorIndices(i) = indicesBB.get(vectorOffset + i * tiling)
      i += 1
    }
    iter += 1
    Vectors.sparse(vectorSize, vectorIndices, vectorValues).asInstanceOf[SparseVector]
  }

  override def haveUnprocessedInputs : Boolean = {
    // True if overrun was non-empty after copying to device
    assert(overrun.isEmpty)
    buffered + tiled > 0
  }

  override def releaseNativeArrays { }

  override def reset() {
    buffered = 0
    iter = 0
    tiled = 0
    valuesBB.clear
    doubleValuesBB.clear
    indicesBB.clear
    intIndicesBB.clear
    currentTileOffset = 0
    overrun = None
  }
}
