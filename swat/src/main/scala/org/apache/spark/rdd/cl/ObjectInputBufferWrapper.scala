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
    val entryPoint : Entrypoint, val blockingCopies : Boolean,
    val selfAllocating : Boolean) extends InputBufferWrapper[T] {
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
  if (selfAllocating) {
    nativeBuffers = generateNativeInputBuffer().asInstanceOf[ObjectNativeInputBuffers[T]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[T] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[T]) {
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

  override def generateNativeInputBuffer() : NativeInputBuffers[T] = {
    new ObjectNativeInputBuffers[T](nele, structSize, blockingCopies,
            constructor, classModel, structMemberTypes, structMemberOffsets)
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val tocopy = if (limit == -1) objCount else limit
    OpenCLBridge.copyJVMArrayToNativeArray(nativeBuffers.buffer, 0, bb.array, 0,
            tocopy * structSize)
    nativeBuffers.tocopy = tocopy
  }

  override def transferOverflowTo(otherAbstract : NativeInputBuffers[T]) :
      NativeInputBuffers[T] = {
    assert(nativeBuffers.tocopy != -1)
    val other : ObjectNativeInputBuffers[T] =
        otherAbstract.asInstanceOf[ObjectNativeInputBuffers[T]]
    val leftover = objCount - nativeBuffers.tocopy

    if (leftover > 0) {
      System.arraycopy(bb.array, 0, bb.array, nativeBuffers.tocopy * structSize,
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

  override def releaseNativeArrays {
    if (selfAllocating) {
      nativeBuffers.releaseNativeArrays
    }
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
