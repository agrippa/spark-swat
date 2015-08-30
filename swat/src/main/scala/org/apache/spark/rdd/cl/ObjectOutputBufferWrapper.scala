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

class ObjectOutputBufferWrapper[T : ClassTag](val className : String, val outArgNum : Int,
        val nLoaded : Int, bbCache : ByteBufferCache, entryPoint : Entrypoint)
        extends OutputBufferWrapper[T] {
  var iter : Int = 0
  val anyFailed : Array[Int] = new Array[Int](1)
  val clazz : java.lang.Class[_] = Class.forName(className)
  val constructor = OpenCLBridge.getDefaultConstructor(clazz)
  val classModel : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      clazz.getName, new NameMatcher(clazz.getName))
  val structMemberTypes : Option[Array[Int]] = if (classModel == null) None else
      Some(classModel.getStructMemberTypes)
  val structMemberOffsets : Option[Array[Long]] = if (classModel == null) None else
      Some(classModel.getStructMemberOffsets)
  val structSize : Int = classModel.getTotalStructSize
  val bb : ByteBuffer = bbCache.getBuffer(structSize * nLoaded)

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

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb)
  }

  override def kernelAttemptCallback(nLoaded : Int, anyFailedArgNum : Int,
          processingSucceededArgnum : Int, outArgNum : Int, heapArgStart : Int,
          heapSize : Int, ctx : Long, dev_ctx : Long, entryPoint : Entrypoint,
          bbCache : ByteBufferCache, devicePointerSize : Int) : Boolean = {
      OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, anyFailedArgNum,
              anyFailed, entryPoint, bbCache)
      anyFailed(0) == 0
  }

  override def finish(ctx : Long, dev_ctx : Long) {
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, outArgNum, bb.array,
            structSize * nLoaded)
  }

  override def countArgumentsUsed() : Int = { 1 }
}
