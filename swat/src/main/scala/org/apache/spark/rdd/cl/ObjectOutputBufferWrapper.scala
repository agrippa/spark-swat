package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._

import java.nio.ByteOrder
import java.nio.ByteBuffer
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

class ObjectOutputBufferWrapper[T : ClassTag](val className : String,
    val N : Int, val entryPoint : Entrypoint)
    extends OutputBufferWrapper[T] {
  var iter : Int = 0
  val clazz : java.lang.Class[_] = Class.forName(className)
  val constructor = OpenCLBridge.getDefaultConstructor(clazz)
  val classModel : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      clazz.getName, new NameMatcher(clazz.getName))
  val structMemberTypes : Option[Array[Int]] = if (classModel == null) None else
      Some(classModel.getStructMemberTypes)
  val structMemberOffsets : Option[Array[Long]] = if (classModel == null) None else
      Some(classModel.getStructMemberOffsets)
  val structSize : Int = classModel.getTotalStructSize
  val bb : ByteBuffer = ByteBuffer.allocate(structSize * N)
  bb.order(ByteOrder.LITTLE_ENDIAN)
  var nLoaded : Int = -1

  override def next() : T = {
    val new_obj : T = constructor.newInstance().asInstanceOf[T]
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb,
            structMemberTypes.get, structMemberOffsets.get)
    iter += 1
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < nLoaded
  }

  override def fillFrom(kernel_ctx : Long,
      nativeOutputBuffers : NativeOutputBuffers[T]) {
    val actual = nativeOutputBuffers.asInstanceOf[ObjectNativeOutputBuffers[T]]
    iter = 0
    bb.clear
    nLoaded = OpenCLBridge.getNLoaded(kernel_ctx)
    assert(nLoaded <= N)
    OpenCLBridge.pinnedToJVMArray(kernel_ctx, bb.array, actual.pinnedBuffer, nLoaded * structSize)
  }

  override def countArgumentsUsed() : Int = { 1 }

  override def generateNativeOutputBuffer(N : Int, outArgNum : Int, dev_ctx : Long,
          ctx : Long, sampleOutput : T, entryPoint : Entrypoint) :
          NativeOutputBuffers[T] = {
    new ObjectNativeOutputBuffers(N, outArgNum, dev_ctx, ctx, entryPoint).asInstanceOf[NativeOutputBuffers[T]]
  }
}
