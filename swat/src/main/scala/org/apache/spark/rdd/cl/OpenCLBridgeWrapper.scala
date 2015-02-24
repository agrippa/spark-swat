package org.apache.spark.rdd.cl

object OpenCLBridgeWrapper {
  def setArrayArg(ctx : Long, argnum : Int, arg : Array[_]) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      throw new RuntimeException("Unsupported type")
    }
  }

  def fetchArrayArg(ctx : Long, argnum : Int, arg : Array[_]) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      throw new RuntimeException("Unsupported type")
    }
  }
}
