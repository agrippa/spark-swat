package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._

import java.io.OutputStream
import java.io.FileOutputStream
import java.util.ArrayList
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import reflect.runtime.{universe => ru}

import org.apache.spark.broadcast.Broadcast;

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector

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
  def checkPreconditions(classModels : java.util.List[HardCodedClassModel]) {
  }

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

  def setObjectTypedArrayArg[T](ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arg : Array[T], typeName : String, isInput : Boolean,
      entryPoint : Entrypoint, broadcastId : Long, rddid : Int,
      partitionid : Int, offset : Int, bbCache : ByteBufferCache) : Int = {
    if (arg.isInstanceOf[scala.runtime.ObjectRef[Array[_]]]) {
      return setObjectTypedArrayArg(ctx, dev_ctx, argnum,
          arg.asInstanceOf[scala.runtime.ObjectRef[Array[_]]].elem, typeName,
          isInput, entryPoint, broadcastId, rddid, partitionid, offset, bbCache)
    } else {
      return setObjectTypedArrayArg(ctx, dev_ctx, argnum, arg, arg.length,
          typeName, isInput, entryPoint, broadcastId, rddid, partitionid, offset,
          bbCache)
    }
  }

  def handleSparseVectorArrayArg[T](argnum : Int, arg : Array[T],
          argLength : Int, entryPoint : Entrypoint, typeName : String,
          ctx : Long, dev_ctx : Long) : Int = {
    /*
     * __global org_apache_spark_mllib_linalg_DenseVector* broadcasted$1
     * __global double *broadcasted$1_values
     * __global int *broadcasted$1_sizes
     * __global int *broadcasted$1_offsets
     * int nbroadcasted$1
     */
    var maxOffset = 0
    var offset = 0
    var i = 0
    while (i < argLength) {
        var j = 0
        // Simulate tiling
        while (j < SparseVectorInputBufferWrapperConfig.tiling && i < argLength) {
            val endingOffset = offset + j +
                (SparseVectorInputBufferWrapperConfig.tiling *
                (arg(i).asInstanceOf[SparseVector].size - 1))
            if (endingOffset > maxOffset) {
                maxOffset = endingOffset
            }
            j += 1
            i += 1
        }
        offset = maxOffset + 1
    }
    val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
        typeName, new NameMatcher(typeName))
    val structSize = c.getTotalStructSize
    val buffer : SparseVectorInputBufferWrapper =
          new SparseVectorInputBufferWrapper(maxOffset + 1, argLength, entryPoint)
    for (i <- 0 until argLength) {
      buffer.append(arg(i).asInstanceOf[SparseVector])
    }
    val argsUsed = buffer.copyToDevice(argnum, ctx, dev_ctx, -1, -1, -1)
    OpenCLBridge.setIntArg(ctx, argnum + argsUsed, argLength)
    return argsUsed + 1
  }

  def handleDenseVectorArrayArg[T](argnum : Int, arg : Array[T],
          argLength : Int, entryPoint : Entrypoint, typeName : String,
          ctx : Long, dev_ctx : Long) : Int = {
    /*
     * __global org_apache_spark_mllib_linalg_DenseVector* broadcasted$1
     * __global double *broadcasted$1_values
     * __global int *broadcasted$1_sizes
     * __global int *broadcasted$1_offsets
     * int nbroadcasted$1
     */
    var maxOffset = 0
    var offset = 0
    var i = 0
    while (i < argLength) {
        var j = 0
        // Simulate tiling
        while (j < DenseVectorInputBufferWrapperConfig.tiling && i < argLength) {
            val endingOffset = offset + j +
                (DenseVectorInputBufferWrapperConfig.tiling *
                (arg(i).asInstanceOf[DenseVector].size - 1))
            if (endingOffset > maxOffset) {
                maxOffset = endingOffset
            }
            j += 1
            i += 1
        }
        offset = maxOffset + 1
    }
    val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
        typeName, new NameMatcher(typeName))
    val structSize = c.getTotalStructSize
    val buffer : DenseVectorInputBufferWrapper =
          new DenseVectorInputBufferWrapper(maxOffset + 1, argLength, entryPoint)
    for (i <- 0 until argLength) {
      buffer.append(arg(i).asInstanceOf[DenseVector])
    }
    val argsUsed = buffer.copyToDevice(argnum, ctx, dev_ctx, -1, -1, -1)
    OpenCLBridge.setIntArg(ctx, argnum + argsUsed, argLength)
    return argsUsed + 1
  }

  def setObjectTypedArrayArg[T](ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arg : Array[T], argLength : Int, typeName : String,
      isInput : Boolean, entryPoint : Entrypoint, broadcastId : Long, rdd : Int,
      partition : Int, offset : Int, bbCache : ByteBufferCache) : Int = {
    if (arg.isInstanceOf[scala.runtime.ObjectRef[Array[_]]]) {
      return setObjectTypedArrayArg(ctx, dev_ctx, argnum,
        arg.asInstanceOf[scala.runtime.ObjectRef[Array[_]]].elem, argLength,
        typeName, isInput, entryPoint, broadcastId, rdd, partition, offset,
        bbCache)
    }

    if (arg(0).isInstanceOf[Tuple2[_, _]]) {
      val sample : Tuple2[_, _] = arg(0).asInstanceOf[Tuple2[_, _]]
      val c : ClassModel =
        entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
            new ObjectMatcher(sample))
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

      val firstMemberBufferLength = firstMemberSize * argLength
      val secondMemberBufferLength = secondMemberSize * argLength

      val firstMemberClassModel : ClassModel =
            entryPoint.getModelFromObjectArrayFieldsClasses(
                    sample._1.getClass.getName,
                    new NameMatcher(sample._1.getClass.getName))
      val secondMemberClassModel : ClassModel =
            entryPoint.getModelFromObjectArrayFieldsClasses(
                    sample._2.getClass.getName,
                    new NameMatcher(sample._2.getClass.getName))

      val bb1 : ByteBuffer = bbCache.getBuffer(firstMemberBufferLength)
      val bb2 : ByteBuffer = bbCache.getBuffer(secondMemberBufferLength)
      // val bb1 : ByteBuffer = ByteBuffer.allocate(firstMemberBufferLength)
      // val bb2 : ByteBuffer = ByteBuffer.allocate(secondMemberBufferLength)
      // bb1.order(ByteOrder.LITTLE_ENDIAN)
      // bb2.order(ByteOrder.LITTLE_ENDIAN)

      for (eleIndex <- 0 until argLength) {
        val ele = arg(eleIndex)
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
          writeTupleMemberToStream(tupleEle._1, bb1, firstMemberClassModel)
        }
        if (secondMemberSize > 0) {
          writeTupleMemberToStream(tupleEle._2, bb2, secondMemberClassModel)
        }
      }

      if (firstMemberSize > 0) {
        OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum, bb1.array,
            firstMemberBufferLength, broadcastId, rdd, partition, offset, 0)
      } else {
        OpenCLBridge.setNullArrayArg(ctx, argnum)
      }

      if (secondMemberSize > 0) {
        OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum + 1, bb2.array,
            secondMemberBufferLength, broadcastId, rdd, partition, offset, 1)
      } else {
        OpenCLBridge.setNullArrayArg(ctx, argnum + 1)
      }

      bbCache.releaseBuffer(bb1)
      bbCache.releaseBuffer(bb2)

      if (isInput) {
        OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 2,
            structSize * argLength)
        return 3
      } else {
        return 2
      }
    } else if (arg(0).isInstanceOf[DenseVector]) {
      return handleDenseVectorArrayArg[T](argnum, arg, argLength, entryPoint,
              typeName, ctx, dev_ctx)
    } else if (arg(0).isInstanceOf[SparseVector]) {
      return handleSparseVectorArrayArg[T](argnum, arg, argLength, entryPoint,
              typeName, ctx, dev_ctx)
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          typeName, new NameMatcher(typeName))
      val structSize = c.getTotalStructSize

      val bb : ByteBuffer = bbCache.getBuffer(structSize * argLength)

      for (eleIndex <- 0 until argLength) {
        val ele = arg(eleIndex)
        writeObjectToStream(ele.asInstanceOf[java.lang.Object], c, bb)
      }

      OpenCLBridge.setByteArrayArg(ctx, dev_ctx, argnum, bb.array,
          structSize * argLength, broadcastId, rdd, partition, offset, 0)
      bbCache.releaseBuffer(bb)
      return 1
    }
  }

  def writeTupleMemberToStream[T](tupleMember : T,
          bb : ByteBuffer, memberClassModel : ClassModel) {
    tupleMember.getClass.getName match {
      case "java.lang.Integer" => {
        bb.putInt(tupleMember.asInstanceOf[java.lang.Integer].intValue);
      }
      case "java.lang.Float" => {
        bb.putFloat(tupleMember.asInstanceOf[java.lang.Float].floatValue);
      }
      case "java.lang.Double" => {
        bb.putDouble(tupleMember.asInstanceOf[java.lang.Double].doubleValue);
      }
      case _ => {
        if (memberClassModel != null) {
          writeObjectToStream(tupleMember.asInstanceOf[java.lang.Object], memberClassModel, bb)
        } else {
          throw new RuntimeException("Unsupported type " + tupleMember.getClass.getName)
        }
      }
    }
  }

  def writeObjectToStream(ele : java.lang.Object, c : ClassModel, bb : ByteBuffer) {
    val structMemberTypes : Array[Int] = c.getStructMemberTypes
    val structMemberOffsets : Array[Long] = c.getStructMemberOffsets
    assert(structMemberTypes.length == structMemberOffsets.length)

    var i = 0
    while (i < structMemberTypes.length) {
      val typ : Int = structMemberTypes(i)
      val offset : Long = structMemberOffsets(i)

      typ match {
        case ClassModel.INT => { bb.putInt(UnsafeWrapper.getInt(ele, offset)); }
        case ClassModel.FLOAT =>  { bb.putFloat(UnsafeWrapper.getFloat(ele, offset)); }
        case ClassModel.DOUBLE => { bb.putDouble(UnsafeWrapper.getDouble(ele, offset)); }
        case _ => throw new RuntimeException("Unsupported type " + typ);
      }
      i += 1
    }
  }

  def fetchTuple2TypedArrayArg(ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arrLength : Int, typeName : String,
      _1typeName : String, _2typeName : String, entryPoint : Entrypoint,
      bbCache : ByteBufferCache) : Tuple2OutputBufferWrapper[_, _] = {
    val startTime = System.currentTimeMillis
    val c : ClassModel = entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
        new DescMatcher(Array(convertClassNameToDesc(_1typeName),
        convertClassNameToDesc(_2typeName))))
    val structSize : Int = c.getTotalStructSize

    val structMembers : java.util.ArrayList[FieldNameInfo] = c.getStructMembers
    assert(structMembers.size == 2)

    val member0 : FieldNameInfo =
        if (structMembers.get(0).name.equals("_1")) structMembers.get(0) else
            structMembers.get(1)
    val member1 : FieldNameInfo =
        if (structMembers.get(0).name.equals("_1")) structMembers.get(1) else
            structMembers.get(0)
    val bb1 : ByteBuffer = bbCache.getBuffer(entryPoint.getSizeOf(
          member0.desc) * arrLength)
    val bb2 : ByteBuffer = bbCache.getBuffer(entryPoint.getSizeOf(
          member1.desc) * arrLength)

    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, argnum, bb1.array,
            entryPoint.getSizeOf(member0.desc) * arrLength)
    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, argnum + 1, bb2.array,
            entryPoint.getSizeOf(member1.desc) * arrLength)

    new Tuple2OutputBufferWrapper(bb1, bb2, arrLength, member0.desc, member1.desc, entryPoint)
  }

  def fetchObjectTypedArrayArgIntoOutputBuffer(ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arrLength : Int, typeName : String,
      clazz : java.lang.Class[_], entryPoint : Entrypoint,
      bbCache : ByteBufferCache) : OutputBufferWrapper[_] = {
    val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
        typeName, new NameMatcher(typeName))
    val structSize : Int = c.getTotalStructSize

    // assert(!arg(0).isInstanceOf[Tuple2[_, _]])
    // val bb : ByteBuffer = ByteBuffer.allocate(structSize * arrLength)
    // bb.order(ByteOrder.LITTLE_ENDIAN)
    val bb : ByteBuffer = bbCache.getBuffer(structSize * arrLength)

    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, argnum, bb.array, structSize * arrLength)

    new ObjectOutputBufferWrapper(bb, arrLength, c, clazz)
  }

  def fetchObjectTypedArrayArg(ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arg : Array[_], typeName : String, entryPoint : Entrypoint,
      bbCache : ByteBufferCache) {
    val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
        typeName, new NameMatcher(typeName))
    val arrLength : Int = arg.length
    val structSize : Int = c.getTotalStructSize

    assert(!arg(0).isInstanceOf[Tuple2[_, _]])
    val bb : ByteBuffer = bbCache.getBuffer(structSize * arrLength)

    OpenCLBridge.fetchByteArrayArg(ctx, dev_ctx, argnum, bb.array, structSize * arrLength)

    for (ele <- arg) {
      readObjectFromStream(ele, c, bb, c.getStructMemberTypes,
              c.getStructMemberOffsets)
    }
  }

  def readTupleMemberFromStream[T](tupleMemberDesc : String,
      bb : ByteBuffer, clazz : Class[_], memberClassModel : Option[ClassModel],
      constructedObj : Option[T], structMemberTypes : Option[Array[Int]],
      structMemberOffsets : Option[Array[Long]]) : T = {
    tupleMemberDesc match {
      case "I" => { return new java.lang.Integer(bb.getInt).asInstanceOf[T] }
      case "F" => { return new java.lang.Float(bb.getFloat).asInstanceOf[T] }
      case "D" => { return new java.lang.Double(bb.getDouble).asInstanceOf[T] }
      case _ => {
        if (!memberClassModel.isEmpty) {
          readObjectFromStream(constructedObj.get, memberClassModel.get, bb,
                  structMemberTypes.get, structMemberOffsets.get)
          return constructedObj.get
        } else {
          throw new RuntimeException("Unsupported type " + tupleMemberDesc)
        }
      }
    }
  }

  def readObjectFromStream[T](ele : T, c : ClassModel, bb : ByteBuffer,
      structMemberTypes : Array[Int], structMemberOffsets : Array[Long]) {
    var i = 0
    while (i < structMemberTypes.length) {
      val typ : Int = structMemberTypes(i)
      val offset : Long = structMemberOffsets(i)

      typ match {
        case ClassModel.INT => { UnsafeWrapper.putInt(ele, offset, bb.getInt); }
        case ClassModel.FLOAT =>  { UnsafeWrapper.putFloat(ele, offset, bb.getFloat); }
        case ClassModel.DOUBLE => { UnsafeWrapper.putDouble(ele, offset, bb.getDouble); }
        case _ => throw new RuntimeException("Unsupported type");
      }
      i += 1
    }
  }

  def setArrayArg[T](ctx : scala.Long, dev_ctx : scala.Long, argnum : Int,
      arg : Array[T], argLength : Int, isInput : Boolean,
      entryPoint : Entrypoint, rddid : Int, partitionid : Int, offset : Int,
      bbCache : ByteBufferCache) : Int = {

    if (arg.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, dev_ctx, argnum,
          arg.asInstanceOf[Array[Double]], argLength, -1, rddid, partitionid, offset, 0)
      return 1
    } else if (arg.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum,
          arg.asInstanceOf[Array[Int]], argLength, -1, rddid, partitionid, offset, 0)
      return 1
    } else if (arg.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, dev_ctx, argnum,
          arg.asInstanceOf[Array[Float]], argLength, -1, rddid, partitionid, offset, 0)
      return 1
    } else {
      // Assume is some serializable object array
      val argClass : java.lang.Class[_] = arg(0).getClass
      return setObjectTypedArrayArg(ctx, dev_ctx, argnum, arg, argLength,
          argClass.getName, isInput, entryPoint, -1, rddid, partitionid, offset,
          bbCache)
    }
  }

  def fetchArrayArg[T](ctx : scala.Long, dev_ctx : scala.Long, argnum : Int,
      arg : Array[T], entryPoint : Entrypoint, bbCache : ByteBufferCache) {
    if (arg.isInstanceOf[Array[Double]]) {
      val casted : Array[Double] = arg.asInstanceOf[Array[Double]]
      OpenCLBridge.fetchDoubleArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
    } else if (arg.isInstanceOf[Array[Int]]) {
      val casted : Array[Int] = arg.asInstanceOf[Array[Int]]
      OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
    } else if (arg.isInstanceOf[Array[Float]]) {
      val casted : Array[Float] = arg.asInstanceOf[Array[Float]]
      OpenCLBridge.fetchFloatArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
    } else if (arg.isInstanceOf[Array[Tuple2[_, _]]]) {
      throw new RuntimeException("This code path does not support Tuple2 anymore")
    } else {
      fetchObjectTypedArrayArg(ctx, dev_ctx, argnum, arg, arg(0).getClass.getName,
          entryPoint, bbCache)
    }
  }

  def setUnitializedArrayArg[U](ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, N : Int, clazz : java.lang.Class[_],
      entryPoint : Entrypoint, sampleOutput : U) : Int = {
    if (clazz.equals(classOf[Double])) {
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 8 * N)
      return 1
    } else if (clazz.equals(classOf[Int])) {
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 4 * N)
      return 1
    } else if (clazz.equals(classOf[Float])) {
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 4 * N)
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

      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, arrsize0)
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 1, arrsize1)

      return 2
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          clazz.getName, new NameMatcher(clazz.getName))
      val structSize : Int = c.getTotalStructSize
      val nbytes : Int = structSize * N
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, nbytes)
      return 1
    }
  }

  def fetchArgFromUnitializedArray[T : ClassTag](ctx : scala.Long,
      dev_ctx : scala.Long, argnum : Int, N : Int,
      entryPoint : Entrypoint, sampleOutput : T, bbCache : ByteBufferCache) :
      OutputBufferWrapper[T] = {
    val clazz : java.lang.Class[_] = classTag[T].runtimeClass

    if (clazz.equals(classOf[Double])) {
      val casted : Array[Double] = new Array[Double](N)
      OpenCLBridge.fetchDoubleArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
      new PrimitiveOutputBufferWrapper[Double](casted).asInstanceOf[OutputBufferWrapper[T]]
    } else if (clazz.equals(classOf[Int])) {
      val casted : Array[Int] = new Array[Int](N)
      OpenCLBridge.fetchIntArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
      new PrimitiveOutputBufferWrapper[Int](casted).asInstanceOf[OutputBufferWrapper[T]]
    } else if (clazz.equals(classOf[Float])) {
      val casted : Array[Float] = new Array[Float](N)
      OpenCLBridge.fetchFloatArrayArg(ctx, dev_ctx, argnum, casted, casted.length)
      new PrimitiveOutputBufferWrapper[Float](casted).asInstanceOf[OutputBufferWrapper[T]]
    } else if (clazz.equals(classOf[Tuple2[_, _]])) {
      val sampleTuple : Tuple2[_, _] = sampleOutput.asInstanceOf[Tuple2[_, _]]
      fetchTuple2TypedArrayArg(ctx, dev_ctx, argnum,
          N, clazz.getName,
          sampleTuple._1.getClass.getName, sampleTuple._2.getClass.getName,
          entryPoint, bbCache).asInstanceOf[OutputBufferWrapper[T]]
    } else {
      fetchObjectTypedArrayArgIntoOutputBuffer(ctx, dev_ctx, argnum, N,
            clazz.getName, clazz, entryPoint, bbCache).asInstanceOf[OutputBufferWrapper[T]]
    }
  }

  def getBroadcastId(obj : java.lang.Object) : Long = {
    return obj.asInstanceOf[Broadcast[_]].id
  }

  /*
   * Based on StackOverflow question "How can I get the memory location of a
   * object in java?"
   */
  def addressOfContainedArray(wrapper : Array[java.lang.Object],
          baseWrapperOffset : Long, baseOffset : Long) : Long = {
    // Get the address of arr as if it were a long field
    val arrAddress : Long = UnsafeWrapper.getLong(wrapper, baseWrapperOffset)
    arrAddress + baseOffset
  }

}
