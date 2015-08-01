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
import com.amd.aparapi.internal.model.HardCodedClassModel
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.ClassModelMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.HardCodedClassModelMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels.DescMatcher
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.instruction.InstructionSet.TypeSpec

class ObjectMatcher(sample : Tuple2[_, _]) extends HardCodedClassModelMatcher {

  def matches(genericModel : HardCodedClassModel) : Boolean = {
    val model : Tuple2ClassModel = genericModel.asInstanceOf[Tuple2ClassModel]
    val firstMatches =
        OpenCLBridgeWrapper.convertClassNameToDesc(
          sample._1.getClass.getName).equals(model.getTypeParamDescs.get(0))
    val secondMatches =
        OpenCLBridgeWrapper.convertClassNameToDesc(
          sample._2.getClass.getName).equals(model.getTypeParamDescs.get(1))
    firstMatches && secondMatches
  }
}

object OpenCLBridgeWrapper {
  def convertClassNameToDesc(className : String) : String = {
    className match {
      case "java.lang.Integer" => "I";
      case "java.lang.Float" => "F";
      case "java.lang.Double" => "D";
      case _ => {
        if (className.length == 1) className
        else "L" + className + ";"
      }
    }
  }

  def getArrayLength(arg : Array[_]) : Int = {
    if (arg.isInstanceOf[scala.runtime.ObjectRef[Array[_]]]) {
      return getArrayLength(arg.asInstanceOf[scala.runtime.ObjectRef[Array[_]]].elem)
    } else {
      return arg.length
    }
  }

  def unwrapBroadcastedArray(obj : java.lang.Object) : java.lang.Object = {
    return obj.asInstanceOf[org.apache.spark.broadcast.Broadcast[_]].value.asInstanceOf[java.lang.Object]
  }

  def setObjectTypedArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T],
      typeName : String, isInput : Boolean, entryPoint : Entrypoint) : Int = {
    if (arg.isInstanceOf[scala.runtime.ObjectRef[Array[T]]]) {
      return setObjectTypedArrayArg(ctx, argnum,
        arg.asInstanceOf[scala.runtime.ObjectRef[Array[T]]].elem, typeName,
        isInput, entryPoint)
    }

    if (arg(0).isInstanceOf[Tuple2[_, _]]) {
      val sample : Tuple2[_, _] = arg(0).asInstanceOf[Tuple2[_, _]]
      val c : ClassModel =
        entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
            new ObjectMatcher(sample))
      val arrLength : Int = arg.length
      val structSize = c.getTotalStructSize

      val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
      assert(structMembers.size == 2)

      val desc0 : String = structMembers.get(0).desc
      val desc1 : String = structMembers.get(1).desc
      val name0 : String = structMembers.get(0).name
      val name1 : String = structMembers.get(1).name
      assert(name0.equals("_1") || name1.equals("_1"))

      val size0 = entryPoint.getSizeOf(desc0)
      val size1 = entryPoint.getSizeOf(desc1)

      val firstMemberSize = if (name0.equals("_1")) size0 else size1
      val secondMemberSize = if (name0.equals("_1")) size1 else size0

      val firstMemberBufferLength = firstMemberSize * arrLength
      val secondMemberBufferLength = secondMemberSize * arrLength

      val bb1 : ByteBuffer = ByteBuffer.allocate(firstMemberBufferLength)
      val bb2 : ByteBuffer = ByteBuffer.allocate(secondMemberBufferLength)
      bb1.order(ByteOrder.LITTLE_ENDIAN)
      bb2.order(ByteOrder.LITTLE_ENDIAN)

      for (eleIndex <- 0 until arg.length) {
        val ele : T = arg(eleIndex)
        val tupleEle : Tuple2[_, _] = ele.asInstanceOf[Tuple2[_, _]]

        /*
         * This ordering is important, dont touch it.
         *
         * It is possible that an object-typed field of a Tuple2 has a size of 0
         * if that field is never referenced from the lambda. In that case, we
         * don't need to transfer to the device because we know that memory
         * buffer will never be referenced anyway.
         */
        if (firstMemberSize > 0) {
          writeTupleMemberToStream(tupleEle._1, entryPoint, bb1)
        }
        if (secondMemberSize > 0) {
          writeTupleMemberToStream(tupleEle._2, entryPoint, bb2)
        }
      }

      if (bb1.remaining != 0) {
        throw new RuntimeException("Expected to completely use bb1, but has " +
            bb1.remaining + " bytes left out of a total " +
            firstMemberBufferLength)
      }
      if (bb2.remaining != 0) {
        throw new RuntimeException("Expected to completely use bb2, but has " +
            bb2.remaining + " bytes left out of a total " +
            secondMemberBufferLength)
      }

      if (firstMemberSize > 0) {
        OpenCLBridge.setByteArrayArg(ctx, argnum, bb1.array)
      } else {
        OpenCLBridge.setNullArrayArg(ctx, argnum)
      }

      if (secondMemberSize > 0) {
        OpenCLBridge.setByteArrayArg(ctx, argnum + 1, bb2.array)
      } else {
        OpenCLBridge.setNullArrayArg(ctx, argnum + 1)
      }

      if (isInput) {
        OpenCLBridge.setArgUnitialized(ctx, argnum + 2, structSize * arrLength)
        return 3
      } else {
        return 2
      }
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          typeName, new NameMatcher(typeName))
      val arrLength : Int = arg.length
      val structSize = c.getTotalStructSize

      val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
      bb.order(ByteOrder.LITTLE_ENDIAN)

      for (eleIndex <- 0 until arg.length) {
        val ele : T = arg(eleIndex)
        writeObjectToStream[T](ele, c, bb)
      }

      if (bb.remaining != 0) {
        throw new RuntimeException("Expected to completely use bb, but has " +
            bb.remaining + " bytes left out of a total " +
            (structSize * arrLength))
      }

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
        val memberClassModel : ClassModel =
          entryPoint.getModelFromObjectArrayFieldsClasses(
              tupleMember.getClass.getName,
              new NameMatcher(tupleMember.getClass.getName))
        if (memberClassModel != null) {
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

  def fetchTuple2TypedArrayArg(ctx : scala.Long, argnum : Int,
      arg : Array[Tuple2[_, _]], typeName : String, _1typeName : String,
      _2typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
        new DescMatcher(Array(convertClassNameToDesc(_1typeName),
        convertClassNameToDesc(_2typeName))))
    val arrLength : Int = arg.length
    val structSize : Int = c.getTotalStructSize

    val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
    assert(structMembers.size == 2)

    val member0 : FieldNameInfo =
        if (structMembers.get(0).name.equals("_1")) structMembers.get(0) else
            structMembers.get(1)
    val member1 : FieldNameInfo =
        if (structMembers.get(0).name.equals("_1")) structMembers.get(1) else
            structMembers.get(0)
    val bb1 : ByteBuffer = ByteBuffer.allocate(entryPoint.getSizeOf(
          member0.desc) * arrLength)
    val bb2 : ByteBuffer = ByteBuffer.allocate(entryPoint.getSizeOf(
          member1.desc) * arrLength)
    bb1.order(ByteOrder.LITTLE_ENDIAN)
    bb2.order(ByteOrder.LITTLE_ENDIAN)

    OpenCLBridge.fetchByteArrayArg(ctx, argnum, bb1.array)
    OpenCLBridge.fetchByteArrayArg(ctx, argnum + 1, bb2.array)

    for (i <- 0 until arg.size) {
      arg(i) = (readTupleMemberFromStream(member0.desc, entryPoint, bb1),
          readTupleMemberFromStream(member1.desc, entryPoint, bb2))
    }
  }

  def fetchObjectTypedArrayArg(ctx : scala.Long, argnum : Int, arg : Array[_],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(typeName, new NameMatcher(typeName))
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
        val memberClassModel : ClassModel =
            entryPoint.getModelFromObjectArrayFieldsClasses(clazz.getName,
                new NameMatcher(clazz.getName))
        if (memberClassModel != null) {
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
      throw new RuntimeException("This code path does not support Tuple2 anymore")
    } else {
      fetchObjectTypedArrayArg(ctx, argnum, arg, arg(0).getClass.getName,
          entryPoint)
    }
  }

  def setUnitializedArrayArg[U](ctx : scala.Long, argnum : Int, N : Int,
      clazz : java.lang.Class[_], entryPoint : Entrypoint, sampleOutput : U) : Int = {
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
      val sampleTuple : Tuple2[_, _] = sampleOutput.asInstanceOf[Tuple2[_, _]]
      val matcher = new DescMatcher(Array(convertClassNameToDesc(
          sampleTuple._1.getClass.getName), convertClassNameToDesc(
          sampleTuple._2.getClass.getName)))
      val c : ClassModel = entryPoint.getHardCodedClassModels.getClassModelFor(
          "scala.Tuple2", matcher)
      val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
      assert(structMembers.size == 2)

      val name0 = structMembers.get(0).name
      val name1 = structMembers.get(1).name
      assert(name0.equals("_1") || name1.equals("_1"))
      val size0 = entryPoint.getSizeOf(structMembers.get(0).desc)
      val size1 = entryPoint.getSizeOf(structMembers.get(1).desc)

      val arrsize0 = if (name0.equals("_1")) (size0 * N) else (size1 * N)
      val arrsize1 = if (name0.equals("_1")) (size1 * N) else (size0 * N)

      OpenCLBridge.setArgUnitialized(ctx, argnum, arrsize0)
      OpenCLBridge.setArgUnitialized(ctx, argnum + 1, arrsize1)

      return 2
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          clazz.getName, new NameMatcher(clazz.getName))
      val structSize : Int = c.getTotalStructSize
      val nbytes : Int = structSize * N
      OpenCLBridge.setArgUnitialized(ctx, argnum, nbytes)
      return 1
    }
  }

  def fetchArgFromUnitializedArray[T : ClassTag](ctx : scala.Long, argnum : Int,
      arg : Array[T], entryPoint : Entrypoint, sampleOutput : T) {
    val clazz : java.lang.Class[_] = classTag[T].runtimeClass

    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else if (clazz.equals(classOf[Tuple2[_, _]])) {
      val sampleTuple : Tuple2[_, _] = sampleOutput.asInstanceOf[Tuple2[_, _]]
      fetchTuple2TypedArrayArg(ctx, argnum,
          arg.asInstanceOf[Array[Tuple2[_, _]]], clazz.getName,
          sampleTuple._1.getClass.getName, sampleTuple._2.getClass.getName,
          entryPoint)
    } else {
      for (i <- 0 until arg.size) {
        arg(i) = OpenCLBridge.constructObjectFromDefaultConstructor(
            clazz).asInstanceOf[T]
      }
      fetchObjectTypedArrayArg(ctx, argnum, arg, clazz.getName, entryPoint)
    }
  }
}
