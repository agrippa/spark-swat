/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
      val requiredElementCapacity = getMaxVectorElementIndexFor(
              (i) => arg(i).size, 1, argLength) + 1
      val buffer : SparseVectorInputBufferWrapper =
            new SparseVectorInputBufferWrapper(requiredElementCapacity, argLength, 1,
                    entryPoint, true)
      buffer.selfAllocate(dev_ctx)
      for (i <- 0 until argLength) {
        buffer.append(arg(i))
      }
      buffer.flush
      buffer.setupNativeBuffersForCopy(-1)
      val result = buffer.getCurrentNativeBuffers.copyToDevice(argnum, ctx,
              dev_ctx, cacheID, true)
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
      buffer.selfAllocate(dev_ctx)
      for (i <- 0 until argLength) {
        buffer.append(arg(i).asInstanceOf[DenseVector])
      }
      buffer.flush
      buffer.setupNativeBuffersForCopy(-1)
      val result = buffer.getCurrentNativeBuffers.copyToDevice(argnum, ctx,
              dev_ctx, cacheID, true)
      return result
    }
  }

  def getInputBufferFor[T : ClassTag](argLength : Int, entryPoint : Entrypoint,
          className : String,
          sparseVectorSizeHandler : Option[Function[Int, Int]],
          denseVectorSizeHandler : Option[Function[Int, Int]],
          primitiveArraySizeHandler : Option[Function[Int, Int]],
          denseVectorTiling : Int, sparseVectorTiling : Int,
          vectorLengthHint : Int, blockingCopies : Boolean, sample : T) :
          InputBufferWrapper[T] = {
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
            assert(vectorLengthHint > 0)
            new DenseVectorInputBufferWrapper(vectorLengthHint * argLength,
                    argLength, denseVectorTiling, entryPoint, blockingCopies)
          } else {
            new DenseVectorInputBufferWrapper(
                    getMaxDenseElementIndexFor(denseVectorSizeHandler.get,
                        argLength), argLength, denseVectorTiling, entryPoint,
                        blockingCopies)
          }
      }
      case "org.apache.spark.mllib.linalg.SparseVector" => {
          if (sparseVectorSizeHandler.isEmpty) {
            assert(vectorLengthHint > 0)
            new SparseVectorInputBufferWrapper(vectorLengthHint * argLength,
                    argLength, sparseVectorTiling, entryPoint, blockingCopies)
          } else {
            new SparseVectorInputBufferWrapper(
                    getMaxSparseElementIndexFor(sparseVectorSizeHandler.get,
                        argLength), argLength, sparseVectorTiling, entryPoint, blockingCopies)
          }
      }
      case _ => {
          if (className.startsWith("[")) {
            if (primitiveArraySizeHandler.isEmpty) {
              assert(vectorLengthHint > 0)
              new PrimitiveArrayInputBufferWrapper(vectorLengthHint * argLength,
                      argLength, 1, entryPoint, blockingCopies, sample)
            } else {
              new PrimitiveArrayInputBufferWrapper(getMaxSparseElementIndexFor(
                          primitiveArraySizeHandler.get, argLength), argLength,
                          1, entryPoint, blockingCopies, sample)
            }
          } else {
            new ObjectInputBufferWrapper(argLength, className,
                    entryPoint, blockingCopies)
          }
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
                    Some((i : Int) => arrOfTuples(i)._1.asInstanceOf[SparseVector].size),
                    Some((i : Int) => arrOfTuples(i)._1.asInstanceOf[DenseVector].size),
                    None, isInput, true)
            inputBuffer.selfAllocate(dev_ctx)

            for (eleIndex <- 0 until argLength) {
              inputBuffer.append(arrOfTuples(eleIndex))
            }
            inputBuffer.flush
            inputBuffer.setupNativeBuffersForCopy(-1)
            val result = inputBuffer.nativeBuffers.copyToDevice(startArgnum, ctx,
                    dev_ctx, cacheID, true)
            return result
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
        case ClassModel.LONG => { UnsafeWrapper.putLong(ele, offset, bb.getLong); }
        case _ => throw new RuntimeException("Unsupported type");
      }
      i += 1
    }
  }

  def getOutputBufferFor[T : ClassTag](sampleOutput : T, N : Int,
      entryPoint : Entrypoint, devicePointerSize : Int, heapSize : Int) :
      OutputBufferWrapper[T] = {
    val className : String = sampleOutput.getClass.getName

    if (className.equals("org.apache.spark.mllib.linalg.DenseVector")) {
        new DenseVectorOutputBufferWrapper(N, devicePointerSize, heapSize)
            .asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("org.apache.spark.mllib.linalg.SparseVector")) {
        new SparseVectorOutputBufferWrapper(N, devicePointerSize, heapSize)
            .asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Double")) {
        new PrimitiveOutputBufferWrapper[Double](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Integer")) {
        new PrimitiveOutputBufferWrapper[Int](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.equals("java.lang.Float")) {
        new PrimitiveOutputBufferWrapper[Float](N).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.startsWith("scala.Tuple2")) {
        new Tuple2OutputBufferWrapper(
                sampleOutput.asInstanceOf[Tuple2[_, _]], N, entryPoint,
                devicePointerSize, heapSize).asInstanceOf[OutputBufferWrapper[T]]
    } else if (className.startsWith("[")) {
        new PrimitiveArrayOutputBufferWrapper(N, devicePointerSize, heapSize,
            sampleOutput).asInstanceOf[OutputBufferWrapper[T]]
    } else {
        new ObjectOutputBufferWrapper[T](className, N,
                entryPoint).asInstanceOf[OutputBufferWrapper[T]]
    }
  }

  def getBroadcastId(obj : java.lang.Object) : Long = {
    return obj.asInstanceOf[Broadcast[_]].id
  }
}
