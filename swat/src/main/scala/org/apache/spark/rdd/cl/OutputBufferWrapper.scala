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
  val structMemberInfo : Array[FieldDescriptor] = if (classModel == null) null else classModel.getStructMemberInfoArray

  override def next() : T = {
    val new_obj : T = constructor.newInstance().asInstanceOf[T]
    OpenCLBridgeWrapper.readObjectFromStream(new_obj, classModel, bb, structMemberInfo)
    new_obj
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb)
  }
}

class Tuple2OutputBufferWrapper[K : ClassTag, V : ClassTag](val bb1 : ByteBuffer, val bb2 : ByteBuffer,
    val N : Int, val member0Desc : String, val member1Desc : String,
    val entryPoint : Entrypoint) extends OutputBufferWrapper[Tuple2[K, V]] {
  var iter : Int = 0

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

  val javaLangIntegerClass : Class[java.lang.Integer] = Class.forName(
      "java.lang.Integer").asInstanceOf[Class[java.lang.Integer]]
  val javaLangFloatClass : Class[java.lang.Float] = Class.forName(
      "java.lang.Float").asInstanceOf[Class[java.lang.Float]]
  val javaLangDoubleClass : Class[java.lang.Double] = Class.forName(
      "java.lang.Double").asInstanceOf[Class[java.lang.Double]]
  
  val intValueField : Field = javaLangIntegerClass.getDeclaredField("value")
  intValueField.setAccessible(true)
  val floatValueField : Field = javaLangFloatClass.getDeclaredField("value")
  floatValueField.setAccessible(true)
  val doubleValueField : Field = javaLangDoubleClass.getDeclaredField("value")
  doubleValueField.setAccessible(true)

  val intValueOffset : Long = UnsafeWrapper.objectFieldOffset(intValueField)
  val floatValueOffset : Long = UnsafeWrapper.objectFieldOffset(floatValueField)
  val doubleValueOffset : Long = UnsafeWrapper.objectFieldOffset(doubleValueField)

  val bufLength : Int = 128
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

  /*
   * Based on StackOverflow question "How can I get the memory location of a
   * object in java?"
   */
  def addressOfContainedArray[T : ClassTag](arr : Array[T],
          wrapper : Array[java.lang.Object], baseWrapperOffset : Long, baseOffset : Long) : Long = {
    // Get the address of arr as if it were a long field
    val arrAddress : Long = UnsafeWrapper.getLong(wrapper, baseWrapperOffset)
    arrAddress + baseOffset
  }

  def fillArray[T : ClassTag](arr : Array[T],
          arrWrapper : Array[java.lang.Object], baseWrapperOffset : Long, baseOffset : Long,
          desc : String, clazz : Class[_],
      classModel : Option[ClassModel], bb : ByteBuffer,
      structMemberInfo : Option[Array[FieldDescriptor]]) {
    localIter = 0
    var i : Int = 0

    desc match {
      case "I" => {
        i = OpenCLBridge.setIntArrFromBB(addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, intValueOffset)
        bb.position(bb.position + (4 * i))
      }
      case "F" => {
        i = OpenCLBridge.setFloatArrFromBB(addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, intValueOffset)
        bb.position(bb.position + (4 * i))
      }
      case "D" => {
        i = OpenCLBridge.setDoubleArrFromBB(addressOfContainedArray(arr,
            arrWrapper, baseWrapperOffset, baseOffset), bufLength, bb.array,
            bb.position, bb.remaining, intValueOffset)
        bb.position(bb.position + (8 * i))
      }
      case _ => {
        while (i < bufLength && bb.remaining > 0) {
          OpenCLBridgeWrapper.readObjectFromStream(arr(i), classModel.get, bb, structMemberInfo.get)
          i += 1
        }
      }
    }
    localCount = i
  }

  // val member0Obj : K = member0Desc match {
  //     case "I" => { new java.lang.Integer(0).asInstanceOf[K] }
  //     case "F" => { new java.lang.Float(0.0f).asInstanceOf[K] }
  //     case "D" => { new java.lang.Double(0.0).asInstanceOf[K] }
  //     case _ => { member0Constructor.get.newInstance().asInstanceOf[K] }
  // }
  // val member1Obj : V = member1Desc match {
  //     case "I" => { new java.lang.Integer(0).asInstanceOf[V] }
  //     case "F" => { new java.lang.Float(0.0f).asInstanceOf[V] }
  //     case "D" => { new java.lang.Double(0.0).asInstanceOf[V] }
  //     case _ => { member1Constructor.get.newInstance().asInstanceOf[V] }
  // }
  // val tuple = (member0Obj, member1Obj)

  val structMember0Info : Option[Array[FieldDescriptor]] =
      if (member0ClassModel.isEmpty) None else
          Some(member0ClassModel.get.getStructMemberInfoArray)
  val structMember1Info : Option[Array[FieldDescriptor]] =
      if (member1ClassModel.isEmpty) None else
          Some(member1ClassModel.get.getStructMemberInfoArray)

  fillArray(member0Arr, member0ArrWrapper, member0BaseWrapperOffset, member0BaseOffset, member0Desc, member0Class, member0ClassModel, bb1,
      structMember0Info)
  // fillArray(member1Arr, member1ArrWrapper, member1BaseWrapperOffset, member1BaseOffset, member1Desc, member1Class, member1ClassModel, bb2,
  //     structMember1Info)

  override def next() : Tuple2[K, V] = {
    if (localIter == localCount) {
      fillArray(member0Arr, member0ArrWrapper, member0BaseWrapperOffset, member0BaseOffset, member0Desc, member0Class, member0ClassModel, bb1,
          structMember0Info)
      // fillArray(member1Arr, member1ArrWrapper, member1BaseWrapperOffset, member1BaseOffset, member1Desc, member1Class, member1ClassModel, bb2,
      //     structMember1Info)
    }
    iter += 1
    localIter += 1
    (member0Arr(localIter - 1), member1Arr(localIter - 1))

    // member0Desc match {
    //   case "I" => { UnsafeWrapper.putInt(member0Obj, intValueOffset, bb1.getInt) }
    //   case "F" => { UnsafeWrapper.putFloat(member0Obj, floatValueOffset, bb1.getFloat) }
    //   case "D" => { UnsafeWrapper.putDouble(member0Obj, doubleValueOffset, bb1.getDouble) }
    //   case _ => { OpenCLBridgeWrapper.readObjectFromStream(member0Obj,
    //           member0ClassModel.get, bb1, structMember0Info.get) }
    // }
    // member1Desc match {
    //   case "I" => { UnsafeWrapper.putInt(member1Obj, intValueOffset, bb2.getInt) }
    //   case "F" => { UnsafeWrapper.putFloat(member1Obj, floatValueOffset, bb2.getFloat) }
    //   case "D" => { UnsafeWrapper.putDouble(member1Obj, doubleValueOffset, bb2.getDouble) }
    //   case _ => { OpenCLBridgeWrapper.readObjectFromStream(member1Obj,
    //           member1ClassModel.get, bb2, structMember1Info.get) }
    // }

    // tuple

    // (OpenCLBridgeWrapper.readTupleMemberFromStream[K](member0Desc, bb1,
    //                                                member0Class,
    //                                                member0ClassModel,
    //                                                member0Obj, structMember0Info),
    //  OpenCLBridgeWrapper.readTupleMemberFromStream[V](member1Desc, bb2,
    //                                                member1Class,
    //                                                member1ClassModel,
    //                                                member1Obj, structMember1Info))
  }

  override def hasNext() : Boolean = {
    iter < N
  }

  override def releaseBuffers(bbCache : ByteBufferCache) {
    bbCache.releaseBuffer(bb1)
    bbCache.releaseBuffer(bb2)
  }
}
