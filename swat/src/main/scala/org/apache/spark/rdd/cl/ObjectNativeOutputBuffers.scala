package org.apache.spark.rdd.cl

import scala.reflect._

import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel

class ObjectNativeOutputBuffers[T : ClassTag](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long, val entryPoint : Entrypoint)
        extends NativeOutputBuffers[T] {
  val clazz : java.lang.Class[_] = classTag[T].runtimeClass
  val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      clazz.getName, new NameMatcher(clazz.getName))
  val structSize : Int = c.getTotalStructSize
  val nbytes : Int = structSize * N

  val clBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * structSize)
  val pinnedBuffer : Long = OpenCLBridge.pin(dev_ctx, clBuffer)

  override def addToArgs() {
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum, clBuffer)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clBuffer, dev_ctx)
  }
}

