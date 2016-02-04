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
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper

import java.nio.ByteBuffer

class ObjectInputBufferWrapper[T](val nele : Int, val typeName : String,
    val entryPoint : Entrypoint, val blockingCopies : Boolean) extends InputBufferWrapper[T] {
  val clazz : java.lang.Class[_] = Class.forName(typeName)
  val constructor = OpenCLBridge.getDefaultConstructor(clazz)
  val classModel : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      typeName, new NameMatcher(typeName))
  val structMemberTypes : Option[Array[Int]] = if (classModel == null) None else
      Some(classModel.getStructMemberTypes)
  val structMemberOffsets : Option[Array[Long]] = if (classModel == null) None else
      Some(classModel.getStructMemberOffsets)
  val structSize = classModel.getTotalStructSize

  var objCount : Int = 0

  val bb : ByteBuffer = ByteBuffer.allocate(nele * structSize)
  bb.order(ByteOrder.LITTLE_ENDIAN)

  var nativeBuffers : ObjectNativeInputBuffers[T] = null

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[ObjectNativeInputBuffers[T]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[T] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[ObjectNativeInputBuffers[T]]
  }

  override def flush() { }

  override def append(obj : Any) {
    OpenCLBridgeWrapper.writeObjectToStream(obj.asInstanceOf[java.lang.Object],
        classModel, bb)
    objCount += 1
  }

  override def aggregateFrom(iter : Iterator[T]) {
    while (objCount < nele && iter.hasNext) {
      append(iter.next)
    }
  }

  override def nBuffered() : Int = {
    objCount
  }

  override def countArgumentsUsed : Int = { 1 }

  override def haveUnprocessedInputs : Boolean = {
    objCount > 0
  }

  override def outOfSpace : Boolean = {
    objCount >= nele
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[T] = {
    new ObjectNativeInputBuffers[T](nele, structSize, blockingCopies,
            constructor, classModel, structMemberTypes, structMemberOffsets, dev_ctx)
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val tocopy = if (limit == -1) objCount else limit
    OpenCLBridge.copyJVMArrayToNativeArray(nativeBuffers.buffer, 0, bb.array, 0,
            tocopy * structSize)
    nativeBuffers.tocopy = tocopy
  }

  override def transferOverflowTo(otherAbstract : NativeInputBuffers[_]) :
      NativeInputBuffers[T] = {
    assert(nativeBuffers.tocopy != -1)
    val other : ObjectNativeInputBuffers[T] =
        otherAbstract.asInstanceOf[ObjectNativeInputBuffers[T]]
    val leftover = objCount - nativeBuffers.tocopy

    if (leftover > 0) {
      System.arraycopy(bb.array, nativeBuffers.tocopy * structSize, bb.array, 0,
              leftover * structSize)
    }
    // Update number of elements in each native buffer
    other.tocopy = -1

    // Update the number of elements stored in this input buffer
    objCount = leftover
    bb.position(leftover * structSize)

    // Update the current native buffers
    val oldBuffers = nativeBuffers
    nativeBuffers = other
    return oldBuffers
  }

  override def reset() { }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entrypoint : Entrypoint, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 1, persistent)) {
      return 1
    } else {
      return -1
    }
  }
}
