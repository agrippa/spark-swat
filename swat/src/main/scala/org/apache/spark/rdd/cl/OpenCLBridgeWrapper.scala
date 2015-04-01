package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._

import java.io.OutputStream
import java.io.FileOutputStream
import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder

import reflect.runtime.{universe => ru}

import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.instruction.InstructionSet.TypeSpec

object OpenCLBridgeWrapper {

  def getSizeForType(t : TypeSpec) : scala.Long = {
    t match {
      case TypeSpec.I => t.getSize;
      case TypeSpec.F => t.getSize;
      case TypeSpec.D => t.getSize;
      case _ => throw new RuntimeException("Unsupported type")
    }
  }

  def setObjectTypedArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T],
      typeName : String, isInput : Boolean, entryPoint : Entrypoint) : Int = {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length
    val structSize = c.getTotalStructSize

    if (arg(0).isInstanceOf[Tuple2[_, _]]) {
      val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
      assert(structMembers.size == 2)

      val firstMemberBufferLength : Int = entryPoint.getSizeOf(structMembers.get(0).desc) * arrLength
      val secondMemberBufferLength : Int = entryPoint.getSizeOf(structMembers.get(1).desc) * arrLength
      val bb1 : ByteBuffer = ByteBuffer.allocate(firstMemberBufferLength)
      val bb2 : ByteBuffer = ByteBuffer.allocate(secondMemberBufferLength)

      for (eleIndex <- 0 until arg.length) {
        val ele : T = arg(eleIndex)
        val tupleEle : Tuple2[_, _] = ele.asInstanceOf[Tuple2[_, _]]

        // This ordering is important, dont touch it.
        writeTupleMemberToStream(tupleEle._2, entryPoint, bb1)
        writeTupleMemberToStream(tupleEle._1, entryPoint, bb2)
      }

      OpenCLBridge.setByteArrayArg(ctx, argnum, bb1.array)
      OpenCLBridge.setByteArrayArg(ctx, argnum + 1, bb2.array)
      if (isInput) {
        OpenCLBridge.setArgUnitialized(ctx, argnum + 2, structSize * arrLength)
      }
      return 3
    } else {
      val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
      bb.order(ByteOrder.LITTLE_ENDIAN)

      for (eleIndex <- 0 until arg.length) {
        val ele : T = arg(eleIndex)
        writeObjectToStream[T](ele, c, bb)
      }

      assert(bb.remaining() == 0)

      OpenCLBridge.setByteArrayArg(ctx, argnum, bb.array)
      return 1
    }
  }

  def writeTupleMemberToStream[T](tupleMember : T, entryPoint : Entrypoint, bb : ByteBuffer) {
    tupleMember.getClass.getName match {
      case "java.lang.Integer" => { val v = tupleMember.asInstanceOf[java.lang.Integer].intValue; bb.putInt(v); }
      case "java.lang.Float" => { val v = tupleMember.asInstanceOf[java.lang.Float].floatValue; bb.putFloat(v); }
      case "java.lang.Double" => { val v = tupleMember.asInstanceOf[java.lang.Double].doubleValue; bb.putDouble(v); }
      case _ => {
        if (entryPoint.getObjectArrayFieldsClasses.containsKey(tupleMember.getClass.getName)) {
          val memberClassModel : ClassModel = entryPoint.getObjectArrayFieldsClasses.get(tupleMember.getClass.getName)
          writeObjectToStream[T](tupleMember, memberClassModel, bb)
        } else {
          throw new RuntimeException("Unsupported type " + tupleMember.getClass.getName)
        }
      }
    }
  }

  def writeObjectToStream[T](ele : T, c : ClassModel, bb : ByteBuffer) {
    val structMemberInfo : java.util.List[FieldDescriptor] = c.getStructMemberInfo

    val fieldIter : java.util.Iterator[FieldDescriptor] = structMemberInfo.iterator
    while (fieldIter.hasNext) {
      val fieldDesc : FieldDescriptor = fieldIter.next
      val typ : TypeSpec = fieldDesc.typ
      val offset : java.lang.Long = fieldDesc.offset

      typ match {
        case TypeSpec.I => { val v : Int = UnsafeWrapper.getInt(ele, offset); bb.putInt(v); }
        case TypeSpec.F =>  { val v : Float = UnsafeWrapper.getFloat(ele, offset); bb.putFloat(v); }
        case TypeSpec.D => { val v : Double = UnsafeWrapper.getDouble(ele, offset); bb.putDouble(v); }
        case _ => throw new RuntimeException("Unsupported type " + typ);
      }
    }
  }

  def fetchTuple2TypedArrayArg(ctx : scala.Long, argnum : Int, arg : Array[Tuple2[_, _]],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length
    val structSize : Int = c.getTotalStructSize

    val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
    assert(structMembers.size == 2)

    val bb1 : ByteBuffer = ByteBuffer.allocate(entryPoint.getSizeOf(
          structMembers.get(0).desc) * arrLength)
    val bb2 : ByteBuffer = ByteBuffer.allocate(entryPoint.getSizeOf(
          structMembers.get(1).desc) * arrLength)

    OpenCLBridge.fetchByteArrayArg(ctx, argnum, bb1.array)
    OpenCLBridge.fetchByteArrayArg(ctx, argnum + 1, bb2.array)

    for (i <- 0 until arg.size) {
      arg(i) = (readTupleMemberFromStream(structMembers.get(0).desc, entryPoint, bb1),
          readTupleMemberFromStream(structMembers.get(1).desc, entryPoint, bb2))
    }
  }

  def fetchObjectTypedArrayArg(ctx : scala.Long, argnum : Int, arg : Array[_],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length
    val structSize : Int = c.getTotalStructSize

    assert(!arg(0).isInstanceOf[Tuple2[_, _]])
    val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
    bb.order(ByteOrder.LITTLE_ENDIAN)

    OpenCLBridge.fetchByteArrayArg(ctx, argnum, bb.array)

    for (ele <- arg) {
      readObjectFromStream(ele, c, bb)
    }
  }

  def readTupleMemberFromStream[T](tupleMemberDesc : String, entryPoint : Entrypoint,
      bb : ByteBuffer) : T = {
    tupleMemberDesc match {
      case "I" => { return new java.lang.Integer(bb.getInt).asInstanceOf[T] }
      case "F" => { return new java.lang.Float(bb.getFloat).asInstanceOf[T] }
      case "D" => { return new java.lang.Double(bb.getDouble).asInstanceOf[T] }
      case _ => {
        val clazz : Class[_] = CodeGenUtil.getClassForDescriptor(tupleMemberDesc)
        if (entryPoint.getObjectArrayFieldsClasses.containsKey(clazz.getName)) {
          val memberClassModel : ClassModel =
            entryPoint.getObjectArrayFieldsClasses.get(clazz.getName)
          val constructedObj : T = OpenCLBridge.constructObjectFromDefaultConstructor(
              clazz).asInstanceOf[T]
          readObjectFromStream(constructedObj, memberClassModel, bb)
          return constructedObj
        } else {
          throw new RuntimeException("Unsupported type " + tupleMemberDesc)
        }
      }
    }
  }

  def readObjectFromStream[T](ele : T, c : ClassModel, bb : ByteBuffer) {
    val structMemberInfo : java.util.List[FieldDescriptor] = c.getStructMemberInfo
    val fieldIter : java.util.Iterator[FieldDescriptor] = structMemberInfo.iterator
    while (fieldIter.hasNext) {
      val fieldDesc : FieldDescriptor = fieldIter.next
      val typ : TypeSpec = fieldDesc.typ
      val offset : java.lang.Long = fieldDesc.offset

      typ match {
        case TypeSpec.I => { val v : Int = bb.getInt; UnsafeWrapper.putInt(ele, offset, v); }
        case TypeSpec.F =>  { val v : Float = bb.getFloat; UnsafeWrapper.putFloat(ele, offset, v); }
        case TypeSpec.D => { val v : Double = bb.getDouble; UnsafeWrapper.putDouble(ele, offset, v); }
        case _ => throw new RuntimeException("Unsupported type");
      }
    }
  }

  def setArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T],
      isInput : Boolean, entryPoint : Entrypoint) : Int = {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
      return 1
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
      return 1
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
      return 1
    } else {
      // Assume is some serializable object array
      val argClass : java.lang.Class[_] = arg(0).getClass
      return setObjectTypedArrayArg[T](ctx, argnum, arg, argClass.getName,
          isInput, entryPoint)
    }
  }

  def fetchArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T], entryPoint : Entrypoint) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else if (arg.isInstanceOf[Array[Tuple2[_, _]]]) {
      fetchTuple2TypedArrayArg(ctx, argnum,
          arg.asInstanceOf[Array[Tuple2[_, _]]], arg(0).getClass.getName,
          entryPoint)
    } else {
      fetchObjectTypedArrayArg(ctx, argnum, arg, arg(0).getClass.getName,
          entryPoint)
    }
  }

  def setUnitializedArrayArg(ctx : scala.Long, argnum : Int, N : Int,
      clazz : java.lang.Class[_], entryPoint : Entrypoint) : Int = {
    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 8 * N)
      return 1
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 4 * N)
      return 1
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 4 * N)
      return 1
    } else if (clazz.equals(classOf[Tuple2[_, _]])) {
      // TODO
      val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(clazz.getName)
      val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
      assert(structMembers.size == 2)

      OpenCLBridge.setArgUnitialized(ctx, argnum,
          entryPoint.getSizeOf(structMembers.get(0).desc) * N)
      OpenCLBridge.setArgUnitialized(ctx, argnum + 1,
          entryPoint.getSizeOf(structMembers.get(1).desc) * N)

      return 2
    } else {
      val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(clazz.getName)
      val structSize : Int = c.getTotalStructSize
      val nbytes : Int = structSize * N
      OpenCLBridge.setArgUnitialized(ctx, argnum, nbytes)
      return 1
    }
  }

  def fetchArgFromUnitializedArray[T: ClassTag](ctx : scala.Long, argnum : Int,
      arg : Array[T], entryPoint : Entrypoint) {
    val clazz : java.lang.Class[_] = classTag[T].runtimeClass

    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else if (clazz.equals(classOf[Tuple2[_, _]])) {
      fetchTuple2TypedArrayArg(ctx, argnum,
          arg.asInstanceOf[Array[Tuple2[_, _]]], clazz.getName, entryPoint)
    } else {
      for (i <- 0 until arg.size) {
        arg(i) = OpenCLBridge.constructObjectFromDefaultConstructor(
            clazz).asInstanceOf[T]
      }
      fetchObjectTypedArrayArg(ctx, argnum, arg, clazz.getName, entryPoint)
    }
  }
}
