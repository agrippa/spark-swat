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

class Tuple2InputBufferWrapper[K : ClassTag, V : ClassTag](val nele : Int,
        val sample : Tuple2[K, V], entryPoint : Entrypoint) extends
        InputBufferWrapper[Tuple2[K, V]] {
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

  // val chunking : Int = 1024

  // val member0Buffer : Array[K] = new Array[K](chunking)
  // val member0BufferWrapper : Array[java.lang.Object] = Array(member0Buffer)
  // val member0BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(
  //         member0BufferWrapper.getClass)
  // val member0BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(
  //         member0Buffer.getClass)
  // val member0Offsets : Array[Long] = getObjFieldOffsets(firstMemberDesc,
  //         firstMemberClassModel)
  // val member0Sizes : Array[Int] = getObjFieldSizes(firstMemberDesc,
  //         firstMemberClassModel)
  // val member0Size : Int = getTotalSize(firstMemberDesc, firstMemberClassModel)
  // val member0ArrayIndexScale : Int = UnsafeWrapper.arrayIndexScale(member0Buffer.getClass)

  // val member1Buffer : Array[V] = new Array[V](chunking)
  // val member1BufferWrapper : Array[java.lang.Object] = Array(member1Buffer)
  // val member1BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(
  //         member1BufferWrapper.getClass)
  // val member1BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(
  //         member1Buffer.getClass)
  // val member1Offsets : Array[Long] = getObjFieldOffsets(secondMemberDesc,
  //         secondMemberClassModel)
  // val member1Sizes : Array[Int] = getObjFieldSizes(secondMemberDesc,
  //         secondMemberClassModel)
  // val member1Size : Int = getTotalSize(secondMemberDesc, secondMemberClassModel)
  // val member1ArrayIndexScale : Int = UnsafeWrapper.arrayIndexScale(member1Buffer.getClass)

  // var localBuffered : Int = 0

  var buffered :  Int = 0
  // val bb1 : Array[Byte] = new Array[Byte](firstMemberSize * nele)
  // val bb2 : Array[Byte] = new Array[Byte](firstMemberSize * nele)
  // var bb1_position : Int = 0
  // var bb2_position : Int = 0
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

  // def saveToBB() {
  //   if (firstMemberSize > 0) {
  //     OpenCLBridge.writeToBBFromObjArray(
  //             OpenCLBridgeWrapper.addressOfContainedArray(
  //                 member0BufferWrapper, member0BaseWrapperOffset,
  //                 member0BaseOffset), localBuffered, bb1.array, bb1.position,
  //             member0Sizes, member0Offsets, member0Size, member0ArrayIndexScale)
  //     bb1.position(bb1.position + (member0Size * localBuffered))
  //   }
  //   if (secondMemberSize > 0) {
  //     OpenCLBridge.writeToBBFromObjArray(
  //             OpenCLBridgeWrapper.addressOfContainedArray(
  //                 member1BufferWrapper, member1BaseWrapperOffset,
  //                 member1BaseOffset), localBuffered, bb2.array, bb2.position,
  //             member1Sizes, member1Offsets, member1Size, member1ArrayIndexScale)
  //     bb2.position(bb2.position + (member1Size * localBuffered))
  //   }
  // }

  override def flush() {
    // saveToBB()
    // localBuffered = 0
  }

  override def append(obj : Tuple2[K, V]) {
    // if (localBuffered == chunking) {
    //     flush
    // }

    if (firstMemberSize > 0) {
      // member0Buffer(localBuffered) = obj._1
      OpenCLBridgeWrapper.writeTupleMemberToStream[K](obj._1, bb1,
              firstMemberClassModel)
    }
    if (secondMemberSize > 0) {
      // member1Buffer(localBuffered) = obj._2
      OpenCLBridgeWrapper.writeTupleMemberToStream[V](obj._2, bb2,
              secondMemberClassModel)
    }
    // localBuffered += 1
    buffered += 1
  }

  override def aggregateFrom(iter : Iterator[Tuple2[K, V]]) : Int = {
    // /*
    //  * If any objects are buffered in the JVM, dump them to the byte[] to ensure
    //  * we maintain the same ordering of elements in the byte[] buffer
    //  */
    // if (localBuffered > 0) {
    //   flush
    // }

    val startBuffered = buffered;
    if (firstMemberSize > 0 && secondMemberSize > 0) {
      val start = System.currentTimeMillis
      while (hasSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        OpenCLBridgeWrapper.writeTupleMemberToStream[K](obj._1, bb1, firstMemberClassModel)
        OpenCLBridgeWrapper.writeTupleMemberToStream[V](obj._2, bb2, secondMemberClassModel)
        buffered += 1
      }

    } else if (firstMemberSize > 0) {
      while (hasSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        OpenCLBridgeWrapper.writeTupleMemberToStream[K](obj._1, bb1, firstMemberClassModel)
        buffered += 1
      }

    } else if (secondMemberSize > 0) {
      while (hasSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        OpenCLBridgeWrapper.writeTupleMemberToStream[V](obj._2, bb2, secondMemberClassModel)
        buffered += 1
      }
    }

    buffered - startBuffered
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
    buffered = 0

    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 2,
            structSize * nele)
    return 3
  }
}
