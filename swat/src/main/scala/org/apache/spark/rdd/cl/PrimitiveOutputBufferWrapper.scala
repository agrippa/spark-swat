package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

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
