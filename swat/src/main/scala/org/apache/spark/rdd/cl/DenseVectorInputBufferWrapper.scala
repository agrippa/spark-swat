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

object DenseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
}

class DenseVectorInputBufferWrapper(val vectorElementCapacity : Int, val vectorCapacity : Int,
        entryPoint : Entrypoint) extends InputBufferWrapper[DenseVector] {
  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.DenseVector", new UnparameterizedMatcher())
  val denseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0

  val tiling : Int = DenseVectorInputBufferWrapperConfig.tiling
  var tiled : Int = 0
  val to_tile : Array[DenseVector] = new Array[DenseVector](tiling)

  val valuesBB : ByteBuffer = ByteBuffer.allocate(vectorElementCapacity * 8)
  valuesBB.order(ByteOrder.LITTLE_ENDIAN)
  val doubleValuesBB : DoubleBuffer = valuesBB.asDoubleBuffer
  var currentTileOffset : Int = 0

  val sizes : Array[Int] = new Array[Int](vectorCapacity)
  val offsets : Array[Int] = new Array[Int](vectorCapacity)

  def calcTileEleStartingOffset(ele : Int) : Int = {
    currentTileOffset + ele
  }

  // inclusive
  def calcTileEleEndingOffset(ele : Int) : Int = {
    calcTileEleStartingOffset(ele) + (tiling * (to_tile(ele).size - 1))
  }

  def outOfValueSpace() : Boolean = {
    for (i <- to_tile.indices) {
      if (calcTileEleEndingOffset(i) >= vectorElementCapacity) {
        return true
      }
    }
    return false
  }

  override def hasSpace() : Boolean = {
    /*
     * The next call to append will force the serialization of the current
     * tile because the current tile is now full. We want to be sure that
     * doesn't overflow the values BB.
     */
    if (tiled == tiling && outOfValueSpace) {
      false
    }
    buffered + tiled < vectorCapacity
  }

  override def flush() {
      var maximumOffsetUsed = 0
      for (i <- to_tile.indices) {
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

  override def append(obj : DenseVector) {
    if (tiled == tiling) {
        flush
    }

    to_tile(tiled) = obj
    tiled += 1
  }

  override def aggregateFrom(iter : Iterator[DenseVector]) : Int = {
    val startBuffered = buffered + tiled
    while (hasSpace && iter.hasNext) {
      val obj : DenseVector = iter.next
      append(obj)
    }
    buffered + tiled - startBuffered
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
          rddid : Int, partitionid : Int, offset : Int) : Int = {
    if (tiled > 0) {
      flush
    }

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, denseVectorStructSize * vectorCapacity)
    // values array, size of double = 8
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 1,
            valuesBB.array, currentTileOffset, 8, -1, rddid,
            partitionid, offset, 1)
    // Sizes of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 2, sizes, buffered, -1,
            rddid, partitionid, offset, 2)
    // Offsets of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 3, offsets, buffered, -1,
            rddid, partitionid, offset, 3)
    buffered = 0

    return 4
  }
}
