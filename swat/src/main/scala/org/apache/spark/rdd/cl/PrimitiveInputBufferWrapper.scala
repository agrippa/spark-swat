package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.writer.KernelWriter

import java.nio.ByteBuffer

class PrimitiveInputBufferWrapper[T: ClassTag](val N : Int,
    val blockingCopies : Boolean)
    extends InputBufferWrapper[T]{
  val arr : Array[T] = new Array[T](N)
  val eleSize : Int = if (arr.isInstanceOf[Array[Double]]) 8 else 4
  var filled : Int = 0
  var used : Int = -1

  var nativeBuffers : PrimitiveNativeInputBuffers[T] = null

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[PrimitiveNativeInputBuffers[T]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[T] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[T]) {
    nativeBuffers = set.asInstanceOf[PrimitiveNativeInputBuffers[T]]
  }

  override def flush() { }

  override def append(obj : Any) {
    arr(filled) = obj.asInstanceOf[T]
    filled += 1
  }

  override def aggregateFrom(iter : Iterator[T]) {
    while (filled < arr.length && iter.hasNext) {
        arr(filled) = iter.next
        filled += 1
    }
  }

  override def nBuffered() : Int = {
    filled
  }

  override def countArgumentsUsed : Int = { 1 }

  override def haveUnprocessedInputs : Boolean = {
    filled > 0
  }

  override def outOfSpace : Boolean = {
    filled >= N
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    val tocopy = if (limit == -1) filled else limit
    assert(tocopy <= filled)
    OpenCLBridge.copyJVMArrayToNativeArray(nativeBuffers.buffer, 0, arr, 0,
            tocopy * eleSize)
    nativeBuffers.tocopy = tocopy
  }

  override def transferOverflowTo(otherAbstract : NativeInputBuffers[T]) :
      NativeInputBuffers[T] = {
    // setupNativeBuffersForCopy must have been called beforehand
    assert(nativeBuffers.tocopy != -1)

    val other : PrimitiveNativeInputBuffers[T] =
        otherAbstract.asInstanceOf[PrimitiveNativeInputBuffers[T]]
    val leftover = filled - nativeBuffers.tocopy

    if (leftover > 0) {
      System.arraycopy(filled, 0, filled, nativeBuffers.tocopy, leftover)
    }
    other.tocopy = -1

    filled = leftover

    val oldBuffers = nativeBuffers
    nativeBuffers = other
    return oldBuffers
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[T] = {
    new PrimitiveNativeInputBuffers(N, eleSize, blockingCopies, dev_ctx)
  }

  override def releaseNativeArrays {
    nativeBuffers.releaseNativeArrays
  }

  override def reset() { }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 1, persistent)) {
      return 1
    } else {
      return -1
    }
  }
}
