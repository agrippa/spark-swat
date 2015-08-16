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

  val bufLength : Int = 4096
  var localIter : Int = 0
  var localCount : Int = 0
  val member0Arr : Array[K] = new Array[K](bufLength)
  val member1Arr : Array[V] = new Array[V](bufLength)

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

  def fillArray[T : ClassTag](arr : Array[T], desc : String, clazz : Class[_],
      classModel : Option[ClassModel], bb : ByteBuffer,
      structMemberInfo : Option[Array[FieldDescriptor]]) {
    localIter = 0
    var i : Int = 0

    desc match {
      case "I" => {
        while (i < bufLength && bb.remaining > 0) {
          UnsafeWrapper.putInt(arr(i), intValueOffset, bb.getInt)
          i += 1
        }
      }
      case "F" => {
        while (i < bufLength && bb.remaining > 0) {
          UnsafeWrapper.putFloat(arr(i), floatValueOffset, bb.getFloat)
          i += 1
        }
      }
      case "D" => {
        while (i < bufLength && bb.remaining > 0) {
          UnsafeWrapper.putDouble(arr(i), doubleValueOffset, bb.getDouble)
          i += 1
        }
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

  // val member0Obj : Option[K] = if (member0Constructor.isEmpty) None else
  //     Some(member0Constructor.get.newInstance())
  // val member1Obj : Option[V] = if (member1Constructor.isEmpty) None else
  //     Some(member1Constructor.get.newInstance())

  val structMember0Info : Option[Array[FieldDescriptor]] =
      if (member0ClassModel.isEmpty) None else
          Some(member0ClassModel.get.getStructMemberInfoArray)
  val structMember1Info : Option[Array[FieldDescriptor]] =
      if (member1ClassModel.isEmpty) None else
          Some(member1ClassModel.get.getStructMemberInfoArray)

  fillArray(member0Arr, member0Desc, member0Class, member0ClassModel, bb1,
      structMember0Info)
  fillArray(member1Arr, member1Desc, member1Class, member1ClassModel, bb2,
      structMember1Info)

  override def next() : Tuple2[K, V] = {
    if (localIter == localCount) {
      fillArray(member0Arr, member0Desc, member0Class, member0ClassModel, bb1,
          structMember0Info)
      fillArray(member1Arr, member1Desc, member1Class, member1ClassModel, bb2,
          structMember1Info)
    }
    iter += 1
    localIter += 1
    (member0Arr(localIter - 1), member1Arr(localIter - 1))
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
