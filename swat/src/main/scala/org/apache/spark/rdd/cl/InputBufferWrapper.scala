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

class Tuple2InputBufferWrapper[K : ClassTag, V : ClassTag](val nele : Int, val sample : Tuple2[K, V],
    entryPoint : Entrypoint) extends InputBufferWrapper[Tuple2[K, V]] {
  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
        new ObjectMatcher(sample))
  val structSize = classModel.getTotalStructSize
  val structMembers : java.util.ArrayList[FieldNameInfo] = classModel.getStructMembers
  assert(structMembers.size == 2)
  assert(structMembers.get(0).name.equals("_1") ||
      structMembers.get(1).name.equals("_1"))

  val desc0 = structMembers.get(0).desc
  val desc1 = structMembers.get(1).desc
  val size0 = entryPoint.getSizeOf(desc0)
  val size1 = entryPoint.getSizeOf(desc1)

  val zeroIsfirst = structMembers.get(0).name.equals("_1")
  val firstMemberDesc = if (zeroIsfirst) desc0 else desc1
  val secondMemberDesc = if (zeroIsfirst) desc1 else desc0
  val firstMemberSize = if (zeroIsfirst) size0 else size1
  val secondMemberSize = if (zeroIsfirst) size1 else size0

  val firstMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._1.getClass.getName,
                new NameMatcher(sample._1.getClass.getName))
  val secondMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._2.getClass.getName,
                new NameMatcher(sample._2.getClass.getName))

  val chunking : Int = 128

  val member0Buffer : Array[K] = new Array[K](chunking)
  val member0BufferWrapper : Array[java.lang.Object] = Array(member0Buffer)
  val member0BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(
          member0BufferWrapper.getClass)
  val member0BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(
          member0Buffer.getClass)
  val member0Offsets : Array[Long] = getObjFieldOffsets(firstMemberDesc,
          firstMemberClassModel)
  val member0Sizes : Array[Int] = getObjFieldSizes(firstMemberDesc,
          firstMemberClassModel)
  val member0Size : Int = getTotalSize(firstMemberDesc, firstMemberClassModel)

  val member1Buffer : Array[V] = new Array[V](chunking)
  val member1BufferWrapper : Array[java.lang.Object] = Array(member1Buffer)
  val member1BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(
          member1BufferWrapper.getClass)
  val member1BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(
          member1Buffer.getClass)
  val member1Offsets : Array[Long] = getObjFieldOffsets(secondMemberDesc,
          secondMemberClassModel)
  val member1Sizes : Array[Int] = getObjFieldSizes(secondMemberDesc,
          secondMemberClassModel)
  val member1Size : Int = getTotalSize(secondMemberDesc, secondMemberClassModel)

  var localBuffered : Int = 0

  var buffered :  Int = 0
  val bb1 : ByteBuffer = ByteBuffer.allocate(firstMemberSize * nele)
  val bb2 : ByteBuffer = ByteBuffer.allocate(secondMemberSize * nele)
  bb1.order(ByteOrder.LITTLE_ENDIAN)
  bb2.order(ByteOrder.LITTLE_ENDIAN)

  def getObjFieldOffsets(desc : String, classModel : ClassModel) : Array[Long] = {
    desc match {
      case "I" => { Array(OpenCLBridgeWrapper.intValueOffset) }
      case "F" => { Array(OpenCLBridgeWrapper.floatValueOffset) }
      case "D" => { Array(OpenCLBridgeWrapper.doubleValueOffset) }
      case _ => {
        classModel.getStructMemberOffsets
      }
    }
  }

  def getObjFieldSizes(desc : String, classModel : ClassModel) : Array[Int] = {
    desc match {
      case "I" => { Array(4) }
      case "F" => { Array(4) }
      case "D" => { Array(8) }
      case _ => {
        classModel.getStructMemberSizes
      }
    }
  }

  def getTotalSize(desc : String, classMOdel : ClassModel) : Int = {
    desc match {
      case "I" => { 4 }
      case "F" => { 4 }
      case "D" => { 8 }
      case _ => {
        classModel.getTotalStructSize
      }
    }
  }

  override def hasSpace() : Boolean = {
    buffered < nele
  }

  def saveToBB() {
    if (firstMemberSize > 0) {
      OpenCLBridge.writeToBBFromObjArray(
              OpenCLBridgeWrapper.addressOfContainedArray(member0Buffer,
                  member0BufferWrapper, member0BaseWrapperOffset,
                  member0BaseOffset), localBuffered, bb1.array, bb1.position,
              member0Sizes, member0Offsets)
          bb1.position(bb1.position + (member0Size * localBuffered))
          // var i = 0
          // while (i < localBuffered) {
          //   OpenCLBridgeWrapper.writeTupleMemberToStream(member0Buffer(i), entryPoint, bb1,
          //           firstMemberClassModel)
          //   i += 1
          // }
    }
    if (secondMemberSize > 0) {
      OpenCLBridge.writeToBBFromObjArray(
              OpenCLBridgeWrapper.addressOfContainedArray(member1Buffer,
                  member1BufferWrapper, member1BaseWrapperOffset,
                  member1BaseOffset), localBuffered, bb2.array, bb2.position,
              member1Sizes, member1Offsets)
          bb2.position(bb2.position + (member1Size * localBuffered))

        // var i = 0
        //     while (i < localBuffered) {
        //         OpenCLBridgeWrapper.writeTupleMemberToStream(member1Buffer(i), entryPoint, bb2,
        //                 secondMemberClassModel)
        //             i += 1
        //     }
    }
    localBuffered = 0
  }

  override def append(obj : Tuple2[K, V]) {
      if (localBuffered == chunking) {
          saveToBB()
      }

      if (firstMemberSize > 0) {
          member0Buffer(localBuffered) = obj._1
              // OpenCLBridgeWrapper.writeTupleMemberToStream(obj._1, entryPoint, bb1,
              //         firstMemberClassModel)
      }
      if (secondMemberSize > 0) {
          member1Buffer(localBuffered) = obj._2
              // OpenCLBridgeWrapper.writeTupleMemberToStream(obj._2, entryPoint, bb2,
              //         secondMemberClassModel)
      }
      localBuffered += 1
          buffered += 1
  }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
          rddid : Int, partitionid : Int, offset : Int) : Int = {
      if (localBuffered > 0) {
          saveToBB()
      }
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
          buffered = 0

          OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 2,
                  structSize * nele)
          return 3
  }
}
