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

import java.lang.{Integer => JavaInteger}
import java.lang.{Float => JavaFloat}
import java.lang.{Double => JavaDouble}

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

  def getArrayLength[T](arg : T) : Int = {
    if (arg.isInstanceOf[Array[_]]) {
      arg.asInstanceOf[Array[_]].length
    } else if (arg.isInstanceOf[scala.runtime.ObjectRef[_]]) {
      getArrayLength(arg.asInstanceOf[scala.runtime.ObjectRef[_]].elem)
    } else {
      throw new RuntimeException("Unexpected type " + arg.getClass.getName)
    }
  }

  def unwrapBroadcastedArray(obj : java.lang.Object) : java.lang.Object = {
    return obj.asInstanceOf[org.apache.spark.broadcast.Broadcast[_]]
        .value.asInstanceOf[java.lang.Object]
  }

  def setObjectTypedArrayArg[T](ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, arg : T, typeName : String, isInput : Boolean,
      entryPoint : Entrypoint, cacheID : CLCacheID) :
      Int = {
    arg match {
      case arr : Array[_] => {
        setObjectTypedArrayArgHelper(ctx, dev_ctx, argnum,
                arg.asInstanceOf[Array[_]], arg.asInstanceOf[Array[_]].length,
                typeName, isInput, entryPoint, cacheID)
      }
      case ref : scala.runtime.ObjectRef[_] => {
        setObjectTypedArrayArg(ctx, dev_ctx, argnum,
                arg.asInstanceOf[scala.runtime.ObjectRef[_]].elem,
                typeName, isInput, entryPoint, cacheID)
      }
      case _ => {
        throw new RuntimeException("Unexpected type " + arg.getClass.getName)
      }
    }
  }

  def getMaxVectorElementIndexFor(getArgSize : Function[Int, Int], tiling : Int,
          argLength : Int) : Int = {
    var maxOffset = 0
    var offset = 0
    var i = 0
    while (i < argLength) {
        var j = 0
        // Simulate tiling
        while (j < tiling && i < argLength) {
            val endingOffset = offset + j + (tiling * (getArgSize(i) - 1))
            if (endingOffset > maxOffset) {
                maxOffset = endingOffset
            }
            j += 1
            i += 1
        }
        offset = maxOffset + 1
    }
    maxOffset
  }

  def getMaxDenseElementIndexFor(getArgSize : Function[Int, Int],
          argLength : Int) : Int = {
    getMaxVectorElementIndexFor(getArgSize,
            DenseVectorInputBufferWrapperConfig.tiling, argLength)
  }

  def getMaxSparseElementIndexFor(getArgSize : Function[Int, Int],
          argLength : Int) : Int = {
    getMaxVectorElementIndexFor(getArgSize,
            SparseVectorInputBufferWrapperConfig.tiling, argLength)
  }

  private def handleSparseVectorArrayArg(argnum : Int, arg : Array[SparseVector],
          argLength : Int, entryPoint : Entrypoint, typeName : String,
          ctx : Long, dev_ctx : Long, cacheID : CLCacheID) : Int = {
    /*
     * __global org_apache_spark_mllib_linalg_DenseVector* broadcasted$1
     * __global double *broadcasted$1_values
     * __global int *broadcasted$1_sizes
     * __global int *broadcasted$1_offsets
     * int nbroadcasted$1
     */
    val cacheSuccess = RuntimeUtil.tryCacheSparseVector(ctx, dev_ctx, argnum,
            cacheID, argLength, 1, entryPoint, true)

    if (cacheSuccess != -1) {
      return cacheSuccess
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          typeName, new NameMatcher(typeName))
      val structSize = c.getTotalStructSize
      val requiredElementCapacity = getMaxSparseElementIndexFor((i) => arg(i).size, argLength) + 1
      val buffer : SparseVectorInputBufferWrapper =
            new SparseVectorInputBufferWrapper(requiredElementCapacity, argLength, 1,
                    entryPoint, true)
      for (i <- 0 until argLength) {
        buffer.append(arg(i))
      }
      buffer.flush
      val result = buffer.copyToDevice(argnum, ctx, dev_ctx, cacheID, true)
      buffer.releaseNativeArrays
      return result
    }
  }

  private def handleDenseVectorArrayArg(argnum : Int, arg : Array[DenseVector],
          argLength : Int, entryPoint : Entrypoint, typeName : String,
          ctx : Long, dev_ctx : Long, cacheID : CLCacheID) : Int = {
    /*
     * __global org_apache_spark_mllib_linalg_DenseVector* broadcasted$1
     * __global double *broadcasted$1_values
     * __global int *broadcasted$1_sizes
     * __global int *broadcasted$1_offsets
     * int nbroadcasted$1
     */
    val cacheSuccess : Int = RuntimeUtil.tryCacheDenseVector(ctx, dev_ctx, argnum,
            cacheID, argLength, 1, entryPoint, true)

    if (cacheSuccess != -1) {
      return cacheSuccess
    } else {
      val requiredElementCapacity = getMaxVectorElementIndexFor(
              (i) => arg(i).size, 1, argLength) + 1
      val buffer : DenseVectorInputBufferWrapper =
              new DenseVectorInputBufferWrapper(requiredElementCapacity,
              argLength, 1, entryPoint, true)
      for (i <- 0 until argLength) {
        buffer.append(arg(i).asInstanceOf[DenseVector])
      }
      buffer.flush
      val result = buffer.copyToDevice(argnum, ctx, dev_ctx, cacheID, true)
      buffer.releaseNativeArrays
      return result
    }
  }

  def getInputBufferFor[T](argLength : Int, entryPoint : Entrypoint,
          className : String, sparseVectorSizeHandler : Option[Function[Int, Int]],
          denseVectorSizeHandler : Option[Function[Int, Int]],
          denseVectorTiling : Int, sparseVectorTiling : Int, blockingCopies : Boolean) : InputBufferWrapper[T] = {
    val result = className match {
      case "java.lang.Integer" => {
          new PrimitiveInputBufferWrapper[Int](argLength, blockingCopies)
      }
      case "java.lang.Float" => {
          new PrimitiveInputBufferWrapper[Float](argLength, blockingCopies)
      }
      case "java.lang.Double" => {
          new PrimitiveInputBufferWrapper[Double](argLength, blockingCopies)
      }
      case "org.apache.spark.mllib.linalg.DenseVector" => {
          if (denseVectorSizeHandler.isEmpty) {
            new DenseVectorInputBufferWrapper(argLength, denseVectorTiling,
                    entryPoint, blockingCopies)
          } else {
            new DenseVectorInputBufferWrapper(
                    getMaxDenseElementIndexFor(denseVectorSizeHandler.get,
                        argLength), argLength, denseVectorTiling, entryPoint,
                        blockingCopies)
          }
      }
      case "org.apache.spark.mllib.linalg.SparseVector" => {
          if (sparseVectorSizeHandler.isEmpty) {
            new SparseVectorInputBufferWrapper(argLength, sparseVectorTiling, entryPoint, blockingCopies)
          } else {
            new SparseVectorInputBufferWrapper(
                    getMaxSparseElementIndexFor(sparseVectorSizeHandler.get,
                        argLength), argLength, sparseVectorTiling, entryPoint, blockingCopies)
          }
      }
      case _ => {
          new ObjectInputBufferWrapper(argLength, className,
                  entryPoint, blockingCopies)
      }
    }
    result.asInstanceOf[InputBufferWrapper[T]]
  }

  /*
   * This method is exclusively used to copy fields of the closure object to the
   * device. These fields can be either a broadcast variable or a regular
   * captured variable. In both cases we want them persistent with blocking copies.
   */
  def setObjectTypedArrayArgHelper[T](ctx : scala.Long, dev_ctx : scala.Long,
      startArgnum : Int, arg : Array[T], argLength : Int, typeName : String,
      isInput : Boolean, entryPoint : Entrypoint, cacheID : CLCacheID) : Int = {

    arg match {
      case denseVectorArray : Array[DenseVector] => {
        return handleDenseVectorArrayArg(startArgnum, denseVectorArray,
                argLength, entryPoint, typeName, ctx, dev_ctx, cacheID)
      }
      case sparseVectorArray : Array[SparseVector] => {
        return handleSparseVectorArrayArg(startArgnum, sparseVectorArray, argLength,
                entryPoint, typeName, ctx, dev_ctx, cacheID)
      }
      case _ => {
        if (arg(0).isInstanceOf[Tuple2[_, _]]) {
          val arrOfTuples : Array[Tuple2[_, _]] = arg.asInstanceOf[Array[Tuple2[_, _]]]
          val sample = arrOfTuples(0)

          val cacheSuccess : Int = RuntimeUtil.tryCacheTuple(ctx, dev_ctx,
              startArgnum, cacheID, argLength, sample, 1, 1, entryPoint, true)
          if (cacheSuccess != -1) {
            return cacheSuccess
          } else {
            val inputBuffer = new Tuple2InputBufferWrapper(
                    argLength, sample, entryPoint,
                    Some((i) => arrOfTuples(i)._1.asInstanceOf[SparseVector].size),
                    Some((i) => arrOfTuples(i)._1.asInstanceOf[DenseVector].size), isInput, true)

            for (eleIndex <- 0 until argLength) {
              inputBuffer.append(arrOfTuples(eleIndex))
            }

            return inputBuffer.copyToDevice(startArgnum, ctx, dev_ctx, cacheID,
                    true)
          }
        } else {
          val cacheSuccess : Int = RuntimeUtil.tryCacheObject(ctx, dev_ctx,
                  startArgnum, cacheID, true)
          if (cacheSuccess != -1) {
            return cacheSuccess
          } else {
            val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
                typeName, new NameMatcher(typeName))
            val structSize = c.getTotalStructSize

            val bb : ByteBuffer = ByteBuffer.allocate(structSize * argLength)
            bb.order(ByteOrder.LITTLE_ENDIAN)

            for (eleIndex <- 0 until argLength) {
              writeObjectToStream[T](arg(eleIndex), c, bb)
            }

            OpenCLBridge.setByteArrayArg(ctx, dev_ctx, startArgnum, bb.array,
                structSize * argLength, cacheID.broadcast, cacheID.rdd,
                cacheID.partition, cacheID.offset, cacheID.component, 0, false)
            return 1
          }
        }
      }
    }
  }

  def writeObjectToStream[T](ele : T, c : ClassModel, bb : ByteBuffer) {
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

  def setUnitializedArrayArg[U](ctx : scala.Long, dev_ctx : scala.Long,
      argnum : Int, N : Int, clazz : java.lang.Class[_],
      entryPoint : Entrypoint, sampleOutput : U, persistent : Boolean) : Int = {
    if (clazz.equals(classOf[Double])) {
      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 8 * N, persistent)) return -1
      return 1
    } else if (clazz.equals(classOf[Int])) {
      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 4 * N, persistent)) return -1
      return 1
    } else if (clazz.equals(classOf[Float])) {
      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, 4 * N, persistent)) return -1
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

      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, arrsize0, persistent) ||
          !OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum + 1, arrsize1, persistent)) {
        return -1
      }

      return 2
    } else {
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          clazz.getName, new NameMatcher(clazz.getName))
      val structSize : Int = c.getTotalStructSize
      val nbytes : Int = structSize * N
      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum, nbytes, persistent)) {
        return -1
      }
      return 1
    }
  }

  def getOutputBufferFor[T : ClassTag](sampleOutput : T, N : Int,
      entryPoint : Entrypoint, devicePointerSize : Int, heapSize : Int) : OutputBufferWrapper[T] = {
    val className : String = sampleOutput.getClass.getName

    if (className.equals("org.apache.spark.mllib.linalg.DenseVector")) {
        new DenseVectorOutputBufferWrapper(N, devicePointerSize, heapSize).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("org.apache.spark.mllib.linalg.SparseVector")) {
        new SparseVectorOutputBufferWrapper(N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Double")) {
        new PrimitiveOutputBufferWrapper[Double](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Integer")) {
        new PrimitiveOutputBufferWrapper[Int](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Float")) {
        new PrimitiveOutputBufferWrapper[Float](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.startsWith("scala.Tuple2")) {
        new Tuple2OutputBufferWrapper(
                sampleOutput.asInstanceOf[Tuple2[_, _]], N,
                entryPoint, devicePointerSize, heapSize).asInstanceOf[OutputBufferWrapper[T]]
    } else {
        new ObjectOutputBufferWrapper[T](className, N,
                entryPoint).asInstanceOf[OutputBufferWrapper[T]]
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
