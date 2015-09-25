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

class PrimitiveOutputBufferWrapper[T : ClassTag](val N : Int)
    extends OutputBufferWrapper[T] {
  var nLoaded : Int = -1
  val arr : Array[T] = new Array[T](N)
  val anyFailed : Array[Int] = new Array[Int](1)
  var iter : Int = 0
  val clazz : java.lang.Class[_] = classTag[T].runtimeClass

  override def next() : T = {
    val index = iter
    iter += 1
    arr(index)
  }

  override def hasNext() : Boolean = {
    iter < nLoaded
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, devicePointerSize : Int) : Boolean = {
      OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, anyFailedArgNum, anyFailed, 1)
      anyFailed(0) == 0
  }

  override def finish(ctx : Long, dev_ctx : Long, outArgNum : Int,
      setNLoaded : Int) {
    nLoaded = setNLoaded
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

  override def countArgumentsUsed() : Int = { 1 }

  override def reset() {
    iter = 0
    nLoaded = -1
  }
}
