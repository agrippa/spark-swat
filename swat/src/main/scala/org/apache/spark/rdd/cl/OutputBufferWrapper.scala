package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

trait OutputBufferWrapper[T] {
  def next() : T
  def hasNext() : Boolean
  def releaseBuffers(bbCache : ByteBufferCache)
}

class PrimitiveOutputBufferWrapper[T](val arr : Array[T])
    extends OutputBufferWrapper[T] {
  var iter : Int = 0

  override def next() : T = {
    val index = iter
    iter += 1
    arr(index)
  }

  override def hasNext() : Boolean = {
    iter < arr.length
  }

  override def releaseBuffers(bbCache : ByteBufferCache) { }
}

class ObjectOutputBufferWrapper[T](val bb : ByteBuffer, val N : Int,
        val classModel : ClassModel, val clazz : java.lang.Class[_])
    extends OutputBufferWrapper[T] {
  var iter : Int = 0
  val constructor = OpenCLBridge.getDefaultConstructor(clazz)
  val structMemberTypes : Option[Array[Int]] = if (classModel == null) None else
      Some(classModel.getStructMemberTypes)
  val structMemberOffsets : Option[Array[Long]] = if (classModel == null) None else
      Some(classModel.getStructMemberOffsets)

  override def next() : T = {
    val new_obj : T = constructor.newInstance().asInstanceOf[T]
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb,
            structMemberTypes.get, structMemberOffsets.get)
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb)
  }
}

class Tuple2OutputBufferWrapper[K : ClassTag, V : ClassTag](
    val bb1 : ByteBuffer, val bb2 : ByteBuffer, val N : Int,
    val member0Desc : String, val member1Desc : String,
    val entryPoint : Entrypoint) extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

  val member0Size : Int = entryPoint.getSizeOf(member0Desc)
  val member1Size : Int = entryPoint.getSizeOf(member1Desc)

  val member0Class : Class[_] = CodeGenUtil.getClassForDescriptor(member0Desc)
  val member1Class : Class[_] = CodeGenUtil.getClassForDescriptor(member1Desc)

  val member0ClassModel : Option[ClassModel] = if (member0Class == null) None else
        Some(entryPoint.getModelFromObjectArrayFieldsClasses(member0Class.getName,
        new NameMatcher(member0Class.getName)))
  val member1ClassModel : Option[ClassModel] = if (member1Class == null) None else
        Some(entryPoint.getModelFromObjectArrayFieldsClasses(member1Class.getName,
        new NameMatcher(member1Class.getName)))

  val member0Constructor : Option[Constructor[K]]= if (member0Class == null) None else
        Some(OpenCLBridge.getDefaultConstructor(member0Class).asInstanceOf[Constructor[K]])
  val member1Constructor : Option[Constructor[V]] = if (member1Class == null) None else
        Some(OpenCLBridge.getDefaultConstructor(member1Class).asInstanceOf[Constructor[V]])

  val bufLength : Int = 512
  var localIter : Int = 0
  var localCount : Int = 0
  val member0Arr : Array[K] = new Array[K](bufLength)
  val member1Arr : Array[V] = new Array[V](bufLength)
  val member0ArrWrapper : Array[java.lang.Object] = Array(member0Arr)
  val member1ArrWrapper : Array[java.lang.Object] = Array(member1Arr)
  val member0BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(member0ArrWrapper.getClass)
  val member1BaseWrapperOffset : Long = UnsafeWrapper.arrayBaseOffset(member1ArrWrapper.getClass)
  val member0BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(member0Arr.getClass)
  val member1BaseOffset : Long = UnsafeWrapper.arrayBaseOffset(member1Arr.getClass)

  def initArray[T : ClassTag](desc : String, arr : Array[T], constructor : Option[Constructor[T]]) {
    desc match {
      case "I" => { for (i <- 0 until bufLength) arr(i) = new java.lang.Integer(0).asInstanceOf[T] }
      case "F" => { for (i <- 0 until bufLength) arr(i) = new java.lang.Float(0.0f).asInstanceOf[T] }
      case "D" => { for (i <- 0 until bufLength) arr(i) = new java.lang.Double(0.0).asInstanceOf[T] }
      case _ => { for (i <- 0 until bufLength) arr(i) = constructor.get.newInstance() }
    }
  }

  initArray(member0Desc, member0Arr, member0Constructor)
  initArray(member1Desc, member1Arr, member1Constructor)

  def fillArray[T : ClassTag](arr : Array[T],
          arrWrapper : Array[java.lang.Object], baseWrapperOffset : Long,
          baseOffset : Long, desc : String, clazz : Class[_],
          classModel : Option[ClassModel], bb : ByteBuffer,
          structMemberTypes : Option[Array[Int]],
          structMemberOffsets : Option[Array[Long]],
          structMemberSizes : Option[Array[Int]], structSize : Int) {
    localIter = 0

    desc match {
      case "I" => {
        localCount = OpenCLBridge.setIntArrFromBB(
            OpenCLBridgeWrapper.addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, OpenCLBridgeWrapper.intValueOffset)
        bb.position(bb.position + (4 * localCount))
      }
      case "F" => {
        localCount = OpenCLBridge.setFloatArrFromBB(
            OpenCLBridgeWrapper.addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, OpenCLBridgeWrapper.floatValueOffset)
        bb.position(bb.position + (4 * localCount))
      }
      case "D" => {
        localCount = OpenCLBridge.setDoubleArrFromBB(
            OpenCLBridgeWrapper.addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, OpenCLBridgeWrapper.doubleValueOffset)
        bb.position(bb.position + (8 * localCount))
      }
      case _ => {
        localCount = OpenCLBridge.setObjectArrFromBB(
            OpenCLBridgeWrapper.addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, structMemberSizes.get,
            structMemberOffsets.get, structSize)
        bb.position(bb.position + (structSize * localCount))
      }
    }
  }

  val structMember0Types : Option[Array[Int]] =
      if (member0ClassModel.isEmpty) None else
          Some(member0ClassModel.get.getStructMemberTypes)
  val structMember0Offsets : Option[Array[Long]] =
      if (member0ClassModel.isEmpty) None else
          Some(member0ClassModel.get.getStructMemberOffsets)
  val structMember0Sizes : Option[Array[Int]] =
      if (member0ClassModel.isEmpty) None else
          Some(member0ClassModel.get.getStructMemberSizes)
  val structMember1Types : Option[Array[Int]] =
      if (member1ClassModel.isEmpty) None else
          Some(member1ClassModel.get.getStructMemberTypes)
  val structMember1Offsets : Option[Array[Long]] =
      if (member1ClassModel.isEmpty) None else
          Some(member1ClassModel.get.getStructMemberOffsets)
  val structMember1Sizes : Option[Array[Int]] =
      if (member1ClassModel.isEmpty) None else
          Some(member1ClassModel.get.getStructMemberSizes)

  fillArray(member0Arr, member0ArrWrapper, member0BaseWrapperOffset,
      member0BaseOffset, member0Desc, member0Class, member0ClassModel, bb1,
      structMember0Types, structMember0Offsets, structMember0Sizes, member0Size)
  fillArray(member1Arr, member1ArrWrapper, member1BaseWrapperOffset,
      member1BaseOffset, member1Desc, member1Class, member1ClassModel, bb2,
      structMember1Types, structMember1Offsets, structMember1Sizes, member1Size)

  override def next() : Tuple2[K, V] = {
    if (localIter == localCount) {
      fillArray(member0Arr, member0ArrWrapper, member0BaseWrapperOffset,
          member0BaseOffset, member0Desc, member0Class, member0ClassModel, bb1,
          structMember0Types, structMember0Offsets, structMember0Sizes, member0Size)
      fillArray(member1Arr, member1ArrWrapper, member1BaseWrapperOffset,
          member1BaseOffset, member1Desc, member1Class, member1ClassModel, bb2,
          structMember1Types, structMember1Offsets, structMember1Sizes, member1Size)
    }
    iter += 1
    localIter += 1
    (member0Arr(localIter - 1), member1Arr(localIter - 1))
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb1)
    bbCache.releaseBuffer(bb2)
  }
}
