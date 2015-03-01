package org.apache.spark.rdd.cl

import java.io.OutputStream
import java.io.FileOutputStream
import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder

import reflect.runtime.{universe => ru}

import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.instruction.InstructionSet.TypeSpec

object OpenCLBridgeWrapper {

  def getSizeForType(t : TypeSpec) : Long = {
    t match {
      case TypeSpec.I => t.getSize;
      case TypeSpec.F => t.getSize;
      case TypeSpec.D => t.getSize;
      case _ => throw new RuntimeException("Unsupported type")
    }
  }

  def setObjectTypedArrayArg(ctx : Long, argnum : Int, arg : Array[_],
      typeName : String, entryPoint : Entrypoint) {
    val c : ClassModel = entryPoint.getObjectArrayFieldsClasses().get(typeName)
    val arrLength : Int = arg.length
    val structMemberTypes : ArrayList[TypeSpec] = c.getStructMemberTypes
    val structMemberOffsets : ArrayList[java.lang.Long] = c.getStructMemberOffsets

    var structSize : Int = c.getTotalStructSize
    val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    var nBytesPut : Long = 0

    for (ele <- arg) {
      for (i <- 0 until structMemberTypes.size) {
        val typ : TypeSpec = structMemberTypes.get(i)
          val offset : Long = structMemberOffsets.get(i)

          typ match {
            case TypeSpec.I => { val v : Int = UnsafeWrapper.getInt(ele, offset); bb.putInt(v); nBytesPut += typ.getSize }
            case TypeSpec.F =>  { val v : Float = UnsafeWrapper.getFloat(ele, offset); bb.putFloat(v); nBytesPut += typ.getSize }
            case TypeSpec.D => { val v : Double = UnsafeWrapper.getDouble(ele, offset); bb.putDouble(v); nBytesPut += typ.getSize }
            case _ => throw new RuntimeException("Unsupported type");
          }
      }
    }

    assert(bb.remaining() == 0)
    assert(nBytesPut == structSize * arrLength)

    OpenCLBridge.setByteArrayArg(ctx, argnum, bb.array)
  }

  def setArrayArg[T](ctx : Long, argnum : Int, arg : Array[T], entryPoint : Entrypoint) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      // Assume is some serializable object array
      setObjectTypedArrayArg(ctx, argnum, arg, arg(0).getClass.getName, entryPoint)
    }
  }

  def fetchArrayArg[T](ctx : Long, argnum : Int, arg : Array[T]) {
    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.fetchDoubleArrayArg(ctx, argnum, arg.asInstanceOf[Array[Double]])
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.fetchIntArrayArg(ctx, argnum, arg.asInstanceOf[Array[Int]])
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.fetchFloatArrayArg(ctx, argnum, arg.asInstanceOf[Array[Float]])
    } else {
      throw new RuntimeException("Unsupported type")
    }
  }
}
