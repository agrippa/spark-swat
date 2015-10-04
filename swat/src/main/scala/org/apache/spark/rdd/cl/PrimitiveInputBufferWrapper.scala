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

class PrimitiveInputBufferWrapper[T: ClassTag](val N : Int) extends InputBufferWrapper[T]{
  val arr : Array[T] = new Array[T](N)
  var filled : Int = 0
  var iter : Int = 0

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

  override def flush() { }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID) : Int = {
    if (arr.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Double]], filled, cacheID.broadcast,
          cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
    } else if (arr.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum,
              arr.asInstanceOf[Array[Int]], filled, cacheID.broadcast,
          cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
    } else if (arr.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Float]], filled, cacheID.broadcast,
          cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component)
    } else {
      throw new RuntimeException("Unsupported")
    }

    return 1
  }

  override def hasNext() : Boolean = {
    iter < filled
  }

  override def next() : T = {
    val n : T = arr(iter)
    iter += 1
    n
  }

  override def haveUnprocessedInputs : Boolean = {
    false
  }

  override def outOfSpace : Boolean = {
    filled >= N
  }

  override def releaseNativeArrays { }

  override def reset() {
    filled = 0
    iter = 0
  }

  // Returns # of arguments used
  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, 0, id.broadcast, id.rdd,
        id.partition, id.offset, id.component, 1)) {
      return 1
    } else {
      return -1
    }
  }
}
