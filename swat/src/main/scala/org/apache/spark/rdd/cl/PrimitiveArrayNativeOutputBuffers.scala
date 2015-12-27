package org.apache.spark.rdd.cl

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher

class PrimitiveArrayNativeOutputBuffers[T](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long,
        val entryPoint : Entrypoint) extends NativeOutputBuffers[T] {

  val clOutBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * 4)
  val pinnedOutBuffer : Long = OpenCLBridge.pin(dev_ctx, clOutBuffer)

  val clIterBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * 4)
  val pinnedIterBuffer : Long = OpenCLBridge.pin(dev_ctx, clIterBuffer)

  override def addToArgs() {
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum, clOutBuffer)
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum + 1, clIterBuffer)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clOutBuffer, dev_ctx)
    OpenCLBridge.clFree(clIterBuffer, dev_ctx)
  }
}

