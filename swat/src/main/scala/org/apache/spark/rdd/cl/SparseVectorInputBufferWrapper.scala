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

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

object SparseVectorInputBufferWrapperConfig {
  val tiling : Int = 32
}

class SparseVectorInputBufferWrapper (val vectorElementCapacity : Int,
        val vectorCapacity : Int, val tiling : Int, val entryPoint : Entrypoint,
        val blockingCopies : Boolean)
        extends InputBufferWrapper[SparseVector] {

  // def this(vectorCapacity : Int, tiling : Int, entryPoint : Entrypoint,
  //         blockingCopies : Boolean) = this(
  //             vectorCapacity * SparseVectorInputBufferWrapperConfig.avgVecLength,
  //             vectorCapacity, tiling, entryPoint, blockingCopies)

  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor(
        "org.apache.spark.mllib.linalg.SparseVector", new UnparameterizedMatcher())
  val sparseVectorStructSize = classModel.getTotalStructSize

  var buffered : Int = 0

  var tiled : Int = 0
  val to_tile : Array[SparseVector] = new Array[SparseVector](tiling)
  val to_tile_sizes : Array[Int] = new Array[Int](tiling)

  var nativeBuffers : SparseVectorNativeInputBuffers = null
  var bufferPosition : Int = 0

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[SparseVectorNativeInputBuffers]
  }

  val overrun : Array[SparseVector] = new Array[SparseVector](tiling)
  var haveOverrun : Boolean = false

  override def getCurrentNativeBuffers : NativeInputBuffers[SparseVector] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[SparseVectorNativeInputBuffers]
  }

  override def flush() {
    if (tiled > 0) {
      val nTiled : Int = OpenCLBridge.serializeStridedSparseVectorsToNativeBuffer(
          nativeBuffers.valuesBuffer, nativeBuffers.indicesBuffer,
          bufferPosition, vectorElementCapacity, nativeBuffers.sizesBuffer,
          nativeBuffers.offsetsBuffer, buffered, vectorCapacity, to_tile, to_tile_sizes,
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
        haveOverrun = true
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

  override def countArgumentsUsed : Int = { 7 }

  override def haveUnprocessedInputs : Boolean = {
    haveOverrun || buffered > 0
  }

  override def outOfSpace : Boolean = {
    haveOverrun
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[SparseVector] = {
    new SparseVectorNativeInputBuffers(vectorElementCapacity, vectorCapacity,
            sparseVectorStructSize, blockingCopies, tiling, dev_ctx)
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val vectorsToCopy = if (limit == -1) buffered else limit
    assert(vectorsToCopy <= buffered)
    val elementsToCopy = if (vectorsToCopy == buffered) bufferPosition else
        OpenCLBridge.getMaxOffsetOfStridedVectors(vectorsToCopy, nativeBuffers.sizesBuffer,
                nativeBuffers.offsetsBuffer, tiling) + 1

    nativeBuffers.vectorsToCopy = vectorsToCopy
    nativeBuffers.elementsToCopy = elementsToCopy
  }

  override def transferOverflowTo(
          otherAbstract : NativeInputBuffers[_]) :
          NativeInputBuffers[SparseVector] = {
    // setupNativeBuffersForCopy must have been called beforehand
    assert(nativeBuffers.vectorsToCopy != -1 && nativeBuffers.elementsToCopy != -1)
    val other : SparseVectorNativeInputBuffers =
        otherAbstract.asInstanceOf[SparseVectorNativeInputBuffers]
    val leftoverVectors = buffered - nativeBuffers.vectorsToCopy
    val leftoverElements = bufferPosition - nativeBuffers.elementsToCopy

    if (leftoverVectors > 0) {
      OpenCLBridge.transferOverflowSparseVectorBuffers(
              other.valuesBuffer, other.indicesBuffer, other.sizesBuffer, other.offsetsBuffer,
              nativeBuffers.valuesBuffer, nativeBuffers.indicesBuffer, nativeBuffers.sizesBuffer,
              nativeBuffers.offsetsBuffer, nativeBuffers.vectorsToCopy, nativeBuffers.elementsToCopy,
              leftoverVectors, leftoverElements)
    }

    // Update number of elements in each native buffer
    other.vectorsToCopy = -1
    other.elementsToCopy = -1

    // Update the number of elements stored in this input buffer
    buffered = leftoverVectors
    bufferPosition = leftoverElements

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
        id.partition, id.offset, id.component, 4, persistent)) {
      val nVectors : Int = OpenCLBridge.fetchNLoaded(id.rdd, id.partition, id.offset)
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.SPARSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.SPARSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, 0,
              c.getTotalStructSize * nVectors, persistent)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, 0 + 5, nVectors)
      // Tiling
      OpenCLBridge.setIntArg(ctx, 0 + 6, tiling)

      return countArgumentsUsed
    } else {
      return -1
    }
  }
}
