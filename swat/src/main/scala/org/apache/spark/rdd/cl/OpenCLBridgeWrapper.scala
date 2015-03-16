package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._

import java.io.OutputStream
import java.io.FileOutputStream
import java.util.ArrayList
import java.util.TreeSet
import java.nio.ByteBuffer
import java.nio.ByteOrder

import reflect.runtime.{universe => ru}

import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
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

  def setObjectTypedArrayArg(ctx : scala.Long, argnum : Int, arg : Array[_],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length
    val structMemberInfo : TreeSet[FieldDescriptor] = c.getStructMemberInfo

    var structSize : Int = c.getTotalStructSize
    val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
    bb.order(ByteOrder.LITTLE_ENDIAN)

    System.err.println("SWAT Setting object typed array with type " + typeName + " arrLength=" + arrLength + " structSize=" + structSize);

    for (eleIndex <- 0 until arg.length) {
      val ele = arg(eleIndex)

      val fieldIter : java.util.Iterator[FieldDescriptor] = structMemberInfo.iterator
      while (fieldIter.hasNext) {
        val fieldDesc : FieldDescriptor = fieldIter.next
        val typ : TypeSpec = fieldDesc.typ
        val offset : java.lang.Long = fieldDesc.offset

        System.err.println(typ.toString() + " " + offset + " " + bb.position() + " " + bb.capacity() + " " + eleIndex);

        typ match {
          case TypeSpec.I => { val v : Int = UnsafeWrapper.getInt(ele, offset); bb.putInt(v); }
          case TypeSpec.F =>  { val v : Float = UnsafeWrapper.getFloat(ele, offset); bb.putFloat(v); }
          case TypeSpec.D => { val v : Double = UnsafeWrapper.getDouble(ele, offset); bb.putDouble(v); }
          case _ => throw new RuntimeException("Unsupported type");
        }
      }
    }

    assert(bb.remaining() == 0)

    OpenCLBridge.setByteArrayArg(ctx, argnum, bb.array)
  }

  def fetchObjectTypedArrayArg(ctx : scala.Long, argnum : Int, arg : Array[_],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length

    val structMemberInfo : TreeSet[FieldDescriptor] = c.getStructMemberInfo
    val structSize : Int = c.getTotalStructSize
    val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
    bb.order(ByteOrder.LITTLE_ENDIAN)

    OpenCLBridge.fetchByteArrayArg(ctx, argnum, bb.array)

    for (ele <- arg) {
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
  }

  def setArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T], entryPoint : Entrypoint) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      // Assume is some serializable object array
      val argClass : java.lang.Class[_] = arg(0).getClass
      setObjectTypedArrayArg(ctx, argnum, arg, argClass.getName, entryPoint)
    }
  }

  def fetchArrayArg[T](ctx : scala.Long, argnum : Int, arg : Array[T], entryPoint : Entrypoint) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      fetchObjectTypedArrayArg(ctx, argnum, arg, arg(0).getClass.getName, entryPoint)
    }
  }

  def setUnitializedArrayArg(ctx : scala.Long, argnum : Int, N : Int,
      clazz : java.lang.Class[_], entryPoint : Entrypoint) {
    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 8 * N)
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 4 * N)
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.setArgUnitialized(ctx, argnum, 4 * N)
    } else {
      val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(clazz.getName)
      val structSize : Int = c.getTotalStructSize
      val nbytes : Int = structSize * N
      OpenCLBridge.setArgUnitialized(ctx, argnum, nbytes)
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
    } else {
      for (i <- 0 until arg.size) {
        arg(i) = OpenCLBridge.constructObjectFromDefaultConstructor(clazz).asInstanceOf[T]
      }
      fetchObjectTypedArrayArg(ctx, argnum, arg, clazz.getName, entryPoint)
    }
  }
}
