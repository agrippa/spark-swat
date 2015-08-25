package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper

import java.nio.ByteBuffer

class ObjectInputBufferWrapper[T](val nele : Int, val typeName : String,
    val entryPoint : Entrypoint) extends InputBufferWrapper[T] {
  val classModel : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      typeName, new NameMatcher(typeName))
  val structSize = classModel.getTotalStructSize
  val bb : ByteBuffer = ByteBuffer.allocate(classModel.getTotalStructSize * nele)
  bb.order(ByteOrder.LITTLE_ENDIAN)

  override def hasSpace() : Boolean = {
    bb.position < bb.capacity
  }

  override def flush() { }

  override def append(obj : T) {
    assert(hasSpace())
    OpenCLBridgeWrapper.writeObjectToStream(obj.asInstanceOf[java.lang.Object], classModel, bb)
  }

  override def aggregateFrom(iter : Iterator[T]) : Int = {
    val startPosition = bb.position
    while (hasSpace && iter.hasNext) {
      OpenCLBridgeWrapper.writeObjectToStream(
              iter.next.asInstanceOf[java.lang.Object], classModel, bb)
    }
    (bb.position - startPosition) / structSize
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int = {
    OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum, bb.array,
        bb.position, -1, rddid, partitionid, offset, 0)
    bb.clear
    return 1
  }
}
