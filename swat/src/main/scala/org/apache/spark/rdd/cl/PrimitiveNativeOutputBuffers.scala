package org.apache.spark.rdd.cl

class PrimitiveNativeOutputBuffers[T : ClassTag](val N : Int,
        val outArgNum : Int, val dev_ctx : Long, val ctx : Long)
        extends NativeOutputBuffers[T] {
  val clazz : java.lang.Class[_] = classTag[T].runtimeClass
  val eleSize : Int = if (clazz.equals(classOf[Double])) 8 else 4

  val clBuffer : Long = OpenCLBridge.clMalloc(dev_ctx, N * eleSize)
  val buffer : Long = OpenCLBridge.pin(dev_ctx, clBuffer)

  override def addToArgs() {
    OpenCLBridge.setOutArrayArg(ctx, dev_ctx, outArgNum, clBuffer)
  }

  override def releaseOpenCLArrays() {
    OpenCLBridge.clFree(clBuffer, dev_ctx)
  }
}

