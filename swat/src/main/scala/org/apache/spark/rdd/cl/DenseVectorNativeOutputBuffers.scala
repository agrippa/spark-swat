package org.apache.spark.rdd.cl

import org.apache.spark.mllib.linalg.DenseVector

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher

class DenseVectorNativeOutputBuffers(val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long,
        val entryPoint : Entrypoint) extends NativeOutputBuffers[DenseVector] {
  val clazz : java.lang.Class[_] = Class.forName(
          "org.apache.spark.mllib.linalg.DenseVector")
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

