package org.apache.spark.rdd.cl

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.lang.reflect.Constructor

import org.apache.spark.mllib.linalg.DenseVector

import com.amd.aparapi.internal.model.ClassModel

class ObjectNativeInputBuffers[T](val N : Int, val structSize : Int,
        val blockingCopies : Boolean, val constructor : Constructor[_],
        val classModel : ClassModel, val structMemberTypes : Option[Array[Int]],
        val structMemberOffsets : Option[Array[Long]]) extends NativeInputBuffers[T] {
  val buffer : Long = OpenCLBridge.nativeMalloc(N * structSize)

  var tocopy : Int = -1
  var iter : Int = 0

  val bb : ByteBuffer = ByteBuffer.allocate(structSize)
  bb.order(ByteOrder.LITTLE_ENDIAN)

  override def releaseNativeArrays() {
    OpenCLBridge.nativeFree(buffer)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      cacheID : CLCacheID, persistent : Boolean) : Int = {
    OpenCLBridge.setNativeArrayArg(ctx, dev_ctx, argnum, buffer,
            tocopy * structSize, cacheID.broadcast, cacheID.rdd,
            cacheID.partition, cacheID.offset, cacheID.component, persistent,
            blockingCopies)
    return 1
  }

  override def next() : T = {
    val new_obj : T = constructor.newInstance().asInstanceOf[T]
    bb.clear
    OpenCLBridge.copyNativeArrayToJVMArray(buffer, iter * structSize, bb.array,
            structSize)
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb,
            structMemberTypes.get, structMemberOffsets.get)
    iter += 1
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < tocopy
  }
}
