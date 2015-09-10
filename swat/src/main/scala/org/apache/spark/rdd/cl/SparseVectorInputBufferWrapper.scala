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

    to_tile(tiled) = obj
    tiled += 1
  }

  override def aggregateFrom(iter : Iterator[SparseVector]) : Int = {
    val startBuffered = buffered + tiled
    while (hasSpace && iter.hasNext) {
      val obj : SparseVector = iter.next
      append(obj)
    }
    buffered + tiled - startBuffered
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
          broadcastId : Int, rddid : Int, partitionid : Int, offset : Int,
          component : Int) : Int = {
    if (tiled > 0) {
      flush
    }

    // Array of structs for each item
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, structSize * vectorCapacity)
    // indices array, size of double = 4
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 1,
            indicesBB.array, currentTileOffset, 4, broadcastId, rddid,
            partitionid, offset, component)
    // values array, size of double = 8
    OpenCLBridge.setArrayArg(ctx, dev_ctx, argnum + 2,
            valuesBB.array, currentTileOffset, 8, broadcastId, rddid,
            partitionid, offset, component + 1)
    // Sizes of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 3, sizes, buffered, broadcastId,
            rddid, partitionid, offset, component + 2)
    // Offsets of each vector
    OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum + 4, offsets, buffered, broadcastId,
            rddid, partitionid, offset, component + 3)
    // Number of sparse vectors being copied
    OpenCLBridge.setIntArg(ctx, argnum + 5, buffered)

    buffered = 0

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

}
