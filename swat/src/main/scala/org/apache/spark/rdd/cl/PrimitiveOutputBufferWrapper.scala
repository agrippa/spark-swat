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

class PrimitiveOutputBufferWrapper[T : ClassTag](val outArgNum : Int,
    val nLoaded : Int) extends OutputBufferWrapper[T] {
  val arr : Array[T] = new Array[T](nLoaded)
  val anyFailed : Array[Int] = new Array[Int](1)
  var iter : Int = 0
  val clazz : java.lang.Class[_] = classTag[T].runtimeClass

  override def next() : T = {
    val index = iter
    iter += 1
    arr(index)
  }

  override def hasNext() : Boolean = {
    iter < arr.length
  }

  override def releaseBuffers(bbCache : ByteBufferCache) { }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
      OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, anyFailedArgNum,
              anyFailed, entryPoint, bbCache)
      anyFailed(0) == 0
  }

  override def finish(ctx : Long, dev_ctx : Long) {
    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, dev_ctx, outArgNum,
              arr.asInstanceOf[Array[Double]], nLoaded)
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, outArgNum,
              arr.asInstanceOf[Array[Int]], nLoaded)
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.fetchFloatArrayArg(ctx, dev_ctx, outArgNum,
              arr.asInstanceOf[Array[Float]], nLoaded)
    } else {
      throw new RuntimeException("Unsupported output primitive type " + clazz.getName)
    }
  }

  def countArgumentsUsed() : Int = { 1 }
}
