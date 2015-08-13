package org.apache.spark.rdd.cl

import java.nio.ByteBuffer

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint

trait OutputBufferWrapper[T] {
  def next() : T
  def hasNext() : Boolean
  def releaseBuffers(bbCache : ByteBufferCache)
}

class PrimitiveOutputBufferWrapper[T](val arr : Array[T])
    extends OutputBufferWrapper[T] {
  var iter : Int = 0

  override def next() : T = {
    val index = iter
    iter += 1
    arr(index)
  }

  override def hasNext() : Boolean = {
    iter < arr.length
  }

  override def releaseBuffers(bbCache : ByteBufferCache) { }
}

class ObjectOutputBufferWrapper[T](val bb : ByteBuffer, val N : Int,
        val classModel : ClassModel, val clazz : java.lang.Class[_])
    extends OutputBufferWrapper[T] {
  var iter : Int = 0

  override def next() : T = {
    val new_obj : T = OpenCLBridge.constructObjectFromDefaultConstructor(clazz).asInstanceOf[T]
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb)
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb)
  }
}

class Tuple2OutputBufferWrapper(val bb1 : ByteBuffer, val bb2 : ByteBuffer,
    val N : Int, val member0Desc : String, val member1Desc : String,
    val entryPoint : Entrypoint) extends OutputBufferWrapper[Tuple2[_, _]] {
  var iter : Int = 0

  override def next() : Tuple2[_, _] = {
    iter += 1
    (OpenCLBridgeWrapper.readTupleMemberFromStream(member0Desc, entryPoint, bb1),
     OpenCLBridgeWrapper.readTupleMemberFromStream(member1Desc, entryPoint, bb2))
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb1)
    bbCache.releaseBuffer(bb2)
  }
}
