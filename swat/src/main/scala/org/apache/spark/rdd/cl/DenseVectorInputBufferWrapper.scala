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

  val startInit = System.currentTimeMillis
  val valuesBB : ByteBuffer = ByteBuffer.allocate(vectorElementCapacity * 8)

  System.err.println("Dense Vector Input Buffer " + (vectorElementCapacity * 8) + " initialization took " +
          (System.currentTimeMillis - startInit) + " ms")

  valuesBB.order(ByteOrder.LITTLE_ENDIAN)
  val doubleValuesBB : DoubleBuffer = valuesBB.asDoubleBuffer
  var currentTileOffset : Int = 0

  val sizes : Array[Int] = new Array[Int](vectorCapacity)
  val offsets : Array[Int] = new Array[Int](vectorCapacity)

  var overrun : Option[DenseVector] = None

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
  def willRunOutOfSpace(next : DenseVector) : Boolean = {
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
      val curr : DenseVector = to_tile(i)
      val startingOffset = calcTileEleStartingOffset(i)
      val endingOffset = calcTileEleEndingOffset(i)

      var currOffset = startingOffset
      for (j <- 0 until curr.size) {
        doubleValuesBB.put(currOffset, curr(j))
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
    append(obj.asInstanceOf[DenseVector])
  }

  def append(obj : DenseVector) {
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

  override def aggregateFrom(iter : Iterator[DenseVector]) : Int = {
    assert(overrun.isEmpty)
    val startBuffered = buffered + tiled
    while (iter.hasNext && overrun.isEmpty) {
      val next : DenseVector = iter.next
      append(next)
    }
    buffered + tiled - startBuffered
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
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 1,
            valuesBB.array, currentTileOffset, 8, cacheID.broadcast,
            cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
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
    currentTileOffset = 0

    if (!overrun.isEmpty) {
      append(overrun.get)
      overrun = None
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
    var i = 0
    while (i < vectorSize) {
      vectorArr(i) = doubleValuesBB.get(vectorOffset + i * tiling)
      i += 1
    }
    iter += 1
    Vectors.dense(vectorArr).asInstanceOf[DenseVector]
  }

  override def haveUnprocessedInputs : Boolean = {
    // True if overrun was non-empty after copying to device
    assert(overrun.isEmpty)
    buffered + tiled > 0
  }
}
