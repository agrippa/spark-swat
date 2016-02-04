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
import scala.reflect._

import java.nio.BufferOverflowException
import java.nio.DoubleBuffer

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.UnparameterizedMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.writer.KernelWriter

object PrimitiveArrayInputBufferWrapperConfig {
  val tiling : Int = 1
}

class PrimitiveArrayInputBufferWrapper[T: ClassTag](val vectorElementCapacity : Int,
        val vectorCapacity : Int, val tiling : Int, val entryPoint : Entrypoint,
        val blockingCopies : Boolean, val firstSample : T) extends InputBufferWrapper[T] {
  var buffered : Int = 0

  val primitiveClass = firstSample.asInstanceOf[Array[_]](0).getClass
  val primitiveElementSize = if (primitiveClass.equals(classOf[java.lang.Integer])) {
            4
          } else if (primitiveClass.equals(classOf[java.lang.Double])) {
            8
          } else if (primitiveClass.equals(classOf[java.lang.Float])) {
            4
          } else if (primitiveClass.equals(classOf[java.lang.Byte])) {
            1
          } else {
              throw new RuntimeException("Unsupported type " + primitiveClass.getName)
          }

  var tiled : Int = 0
  val to_tile : Array[T] = new Array[T](tiling)
  val to_tile_sizes : Array[Int] = new Array[Int](tiling)

  var nativeBuffers : PrimitiveArrayNativeInputBuffers[T] = null
  var bufferPosition : Int = 0

  private val overrun : Array[Option[T]] = new Array[Option[T]](tiling)
  for (i <- 0 until tiling) { overrun(i) = None }
  var haveOverrun : Boolean = false

//   var sumVectorLengths : Int = 0 // PROFILE
//   var countVectors : Int = 0 // PROFILE

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[PrimitiveArrayNativeInputBuffers[T]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[T] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[PrimitiveArrayNativeInputBuffers[T]]
  }

  override def flush() {
    if (tiled > 0) {
      val nToSerialize = if (buffered + tiled > vectorCapacity)
          vectorCapacity - buffered else tiled
      val nTiled : Int =
          OpenCLBridge.serializeStridedPrimitiveArraysToNativeBuffer(
                  nativeBuffers.valuesBuffer, bufferPosition,
                  vectorElementCapacity, nativeBuffers.sizesBuffer,
                  nativeBuffers.offsetsBuffer, buffered, vectorCapacity,
                  to_tile.asInstanceOf[Array[java.lang.Object]], to_tile_sizes,
                  nToSerialize, tiling, primitiveElementSize)
      if (nTiled > 0) {
        var newBufferPosition : Int = bufferPosition + 0 +
            (tiling * (to_tile(0).asInstanceOf[Array[_]].size - 1))

        for (i <- 1 until nTiled) {
          val curr : Array[_] = to_tile(i).asInstanceOf[Array[_]]
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
          overrun(i - nTiled) = Some(to_tile(i))
        }
        haveOverrun = true
      }

      buffered += nTiled
      tiled = 0
    }
  }

  override def append(obj : Any) {
    val arr : T = obj.asInstanceOf[T]
    to_tile(tiled) = arr
    to_tile_sizes(tiled) = arr.asInstanceOf[Array[_]].length
    tiled += 1

//     sumVectorLengths += arr.length // PROFILE
//     countVectors += 1 // PROFILE

    if (tiled == tiling) {
        flush
    }
  }

  override def aggregateFrom(iterator : Iterator[T]) {
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

  override def countArgumentsUsed : Int = { 4 }

  override def haveUnprocessedInputs : Boolean = {
    haveOverrun || buffered > 0
  }

  override def outOfSpace : Boolean = {
    haveOverrun
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[T] = {
    new PrimitiveArrayNativeInputBuffers(vectorElementCapacity, vectorCapacity,
            blockingCopies, tiling, dev_ctx, primitiveElementSize)
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
          NativeInputBuffers[T] = {
    // setupNativeBuffersForCopy must have been called beforehand
    assert(nativeBuffers.vectorsToCopy != -1 && nativeBuffers.elementsToCopy != -1)
    val other : PrimitiveArrayNativeInputBuffers[T] =
        otherAbstract.asInstanceOf[PrimitiveArrayNativeInputBuffers[T]]
    val leftoverVectors = buffered - nativeBuffers.vectorsToCopy
    val leftoverElements = bufferPosition - nativeBuffers.elementsToCopy

    if (leftoverVectors > 0) {
      OpenCLBridge.transferOverflowPrimitiveArrayBuffers(
              other.valuesBuffer, other.sizesBuffer, other.offsetsBuffer,
              nativeBuffers.valuesBuffer, nativeBuffers.sizesBuffer,
              nativeBuffers.offsetsBuffer, nativeBuffers.vectorsToCopy,
              nativeBuffers.elementsToCopy, leftoverVectors, leftoverElements,
              primitiveElementSize)
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
    while (i < tiling && !overrun(i).isEmpty) {
      // TODO what if we run out of space while handling the overrun...
      append(overrun(i).get)
      overrun(i) = None
      i += 1
    }
  }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entrypoint : Entrypoint, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0 + 1, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 3, persistent)) {
      val nVectors : Int = OpenCLBridge.fetchNLoaded(id.rdd, id.partition, id.offset)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, 0 + 3, nVectors)

      return countArgumentsUsed
    } else {
      return -1
    }
  }
}
