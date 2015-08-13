package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo

import java.nio.ByteBuffer

trait InputBufferWrapper[T] {
  def append(obj : T)
  def hasSpace() : Boolean
  def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int
}

class PrimitiveInputBufferWrapper[T: ClassTag](val N : Int) extends InputBufferWrapper[T]{
  val arr : Array[T] = new Array[T](N)
  var filled : Int = 0

  override def hasSpace() : Boolean = {
    filled < arr.length
  }

  override def append(obj : T) {
    assert(hasSpace())
    arr(filled) = obj
    filled += 1
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int = {
    if (arr.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Double]], filled, -1, rddid, partitionid, offset, 0)
    } else if (arr.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Int]], filled, -1, rddid, partitionid, offset, 0)
    } else if (arr.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Float]], filled, -1, rddid, partitionid, offset, 0)
    } else {
      throw new RuntimeException("Unsupported")
    }
    filled = 0
    return 1
  }
}

class ObjectInputBufferWrapper[T](val nele : Int, val typeName : String,
    val entryPoint : Entrypoint) extends InputBufferWrapper[T] {
  val classModel : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
      typeName, new NameMatcher(typeName))
  val bb : ByteBuffer = ByteBuffer.allocate(classModel.getTotalStructSize * nele)
  bb.order(ByteOrder.LITTLE_ENDIAN)

  override def hasSpace() : Boolean = {
    bb.position < bb.capacity
  }

  override def append(obj : T) {
    assert(hasSpace())
    OpenCLBridgeWrapper.writeObjectToStream(obj.asInstanceOf[java.lang.Object], classModel, bb)
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int = {
    OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum, bb.array,
        bb.position, -1, rddid, partitionid, offset, 0)
    bb.clear
    return 1
  }
}

class Tuple2InputBufferWrapper(val nele : Int, val sample : Tuple2[_, _],
    entryPoint : Entrypoint) extends InputBufferWrapper[Tuple2[_, _]] {
  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
        new ObjectMatcher(sample))
  val structSize = classModel.getTotalStructSize
  val structMembers : java.util.ArrayList[FieldNameInfo] = classModel.getStructMembers
  assert(structMembers.size == 2)
  assert(structMembers.get(0).name.equals("_1") ||
      structMembers.get(1).name.equals("_1"))

  val size0 = entryPoint.getSizeOf(structMembers.get(0).desc)
  val size1 = entryPoint.getSizeOf(structMembers.get(1).desc)

  val firstMemberSize = if (structMembers.get(0).name.equals("_1")) size0 else size1
  val secondMemberSize = if (structMembers.get(0).name.equals("_1")) size1 else size0

  val firstMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._1.getClass.getName,
                new NameMatcher(sample._1.getClass.getName))
  val secondMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._2.getClass.getName,
                new NameMatcher(sample._2.getClass.getName))

  val bb1 : ByteBuffer = ByteBuffer.allocate(firstMemberSize * nele)
  val bb2 : ByteBuffer = ByteBuffer.allocate(secondMemberSize * nele)
  bb1.order(ByteOrder.LITTLE_ENDIAN)
  bb2.order(ByteOrder.LITTLE_ENDIAN)

  override def hasSpace() : Boolean = {
    bb1.position < bb1.capacity
  }

  override def append(obj : Tuple2[_, _]) {
    assert(hasSpace())
    if (firstMemberSize > 0) {
      OpenCLBridgeWrapper.writeTupleMemberToStream(obj._1, entryPoint, bb1,
              firstMemberClassModel)
    }
    if (secondMemberSize > 0) {
      OpenCLBridgeWrapper.writeTupleMemberToStream(obj._2, entryPoint, bb2,
              secondMemberClassModel)
    }
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      rddid : Int, partitionid : Int, offset : Int) : Int = {
    if (firstMemberSize > 0) {
      OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum, bb1.array,
          bb1.position, -1, rddid, partitionid, offset, 0)
    } else {
      OpenCLBridge.setNullArrayArg(ctx, argnum)
    }

    if (secondMemberSize > 0) {
      OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum + 1, bb2.array,
          bb2.position, -1, rddid, partitionid, offset, 1)
    } else {
      OpenCLBridge.setNullArrayArg(ctx, argnum + 1)
    }

    bb1.clear
    bb2.clear

    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 2,
        structSize * nele)
    return 3
  }
}
