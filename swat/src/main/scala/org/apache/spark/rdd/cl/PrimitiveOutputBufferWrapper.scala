package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._

import java.nio.ByteBuffer
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

class PrimitiveOutputBufferWrapper[T : ClassTag](val N : Int) extends OutputBufferWrapper[T] {
  var nLoaded : Int = -1
  val arr : Array[T] = new Array[T](N)
  var iter : Int = 0
  val clazz : java.lang.Class[_] = classTag[T].runtimeClass
  val eleSize : Int = if (clazz.equals(classOf[Double])) 8 else 4

  override def next() : T = {
    val index = iter
    iter += 1
    arr(index)
  }

  override def hasNext() : Boolean = {
    iter < nLoaded
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def fillFrom(kernel_ctx : Long, outArgNum : Int) {
    iter = 0
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    OpenCLBridge.nativeToJVMArray(kernel_ctx, arr, outArgNum, nLoaded * eleSize)
  }

  override def getNativeOutputBufferInfo() : Array[Int] = {
    Array(eleSize * N)
  }
}
