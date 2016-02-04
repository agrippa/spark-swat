/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.DoubleBuffer

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.UnparameterizedMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.writer.KernelWriter

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object DenseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
}

class DenseVectorInputBufferWrapper(val vectorElementCapacity : Int,
        val vectorCapacity : Int, val tiling : Int, val entryPoint : Entrypoint,
        val blockingCopies : Boolean)
        extends InputBufferWrapper[DenseVector] {

  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.DenseVector", new UnparameterizedMatcher())
  val denseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0

  var tiled : Int = 0
  val to_tile : Array[DenseVector] = new Array[DenseVector](tiling)
  val to_tile_sizes : Array[Int] = new Array[Int](tiling)

  var nativeBuffers : DenseVectorNativeInputBuffers = null
  var bufferPosition : Int = 0

  private val overrun : Array[DenseVector] = new Array[DenseVector](tiling)
  var haveOverrun : Boolean = false

//   var sumVectorLengths : Int = 0 // PROFILE
//   var countVectors : Int = 0 // PROFILE

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[DenseVectorNativeInputBuffers]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[DenseVector] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[DenseVectorNativeInputBuffers]
  }

  override def flush() {
    if (tiled > 0) {
      val nTiled : Int = OpenCLBridge.serializeStridedDenseVectorsToNativeBuffer(
          nativeBuffers.valuesBuffer,
          bufferPosition, vectorElementCapacity, nativeBuffers.sizesBuffer,
          nativeBuffers.offsetsBuffer, buffered, vectorCapacity, to_tile, to_tile_sizes,
          if (buffered + tiled > vectorCapacity) (vectorCapacity - buffered) else tiled, tiling)
      if (nTiled > 0) {
        var newBufferPosition : Int = bufferPosition + 0 +
            (tiling * (to_tile(0).size - 1))

        for (i <- 1 until nTiled) {
          val curr : DenseVector = to_tile(i)
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

//     sumVectorLengths += obj.size // PROFILE
//     countVectors += 1 // PROFILE

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

  override def countArgumentsUsed : Int = { 6 }

  override def haveUnprocessedInputs : Boolean = {
    haveOverrun || buffered > 0
  }

  override def outOfSpace : Boolean = {
    haveOverrun
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[DenseVector] = {
    new DenseVectorNativeInputBuffers(vectorElementCapacity, vectorCapacity,
            denseVectorStructSize, blockingCopies, tiling, dev_ctx)
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val vectorsToCopy = if (limit == -1) buffered else limit
    assert(vectorsToCopy <= buffered)
    val elementsToCopy = if (vectorsToCopy == buffered) bufferPosition else
        OpenCLBridge.getMaxOffsetOfStridedVectors(vectorsToCopy, nativeBuffers.sizesBuffer,
                nativeBuffers.offsetsBuffer, tiling) + 1
    assert(elementsToCopy <= bufferPosition)

    nativeBuffers.vectorsToCopy = vectorsToCopy
    nativeBuffers.elementsToCopy = elementsToCopy
  }

  override def transferOverflowTo(
          otherAbstract : NativeInputBuffers[_]) :
          NativeInputBuffers[DenseVector] = {
    // setupNativeBuffersForCopy must have been called beforehand
    assert(nativeBuffers.vectorsToCopy != -1 && nativeBuffers.elementsToCopy != -1)
    val other : DenseVectorNativeInputBuffers =
        otherAbstract.asInstanceOf[DenseVectorNativeInputBuffers]
    val leftoverVectors = buffered - nativeBuffers.vectorsToCopy
    val leftoverElements = bufferPosition - nativeBuffers.elementsToCopy

    if (leftoverVectors > 0) {
      OpenCLBridge.transferOverflowDenseVectorBuffers(
              other.valuesBuffer, other.sizesBuffer, other.offsetsBuffer,
              nativeBuffers.valuesBuffer, nativeBuffers.sizesBuffer,
              nativeBuffers.offsetsBuffer, nativeBuffers.vectorsToCopy,
              nativeBuffers.elementsToCopy, leftoverVectors, leftoverElements)
    }

    // Update number of elements in each native buffer
    other.vectorsToCopy = -1
    other.elementsToCopy = -1

    // Update the number of elements stored in this input buffer
    buffered = leftoverVectors
    bufferPosition = leftoverElements

//     System.err.println("Average vector length = " + // PROFILE
//             (sumVectorLengths.toDouble / countVectors.toDouble)) // PROFILE
//     sumVectorLengths = 0 // PROFILE
//     countVectors = 0 // PROFILE

    // Update the current native buffers
    val oldBuffers = nativeBuffers
    nativeBuffers = other
    return oldBuffers
  }

  override def reset() {
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
