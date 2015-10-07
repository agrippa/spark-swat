package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper

import java.nio.ByteBuffer

class Tuple2InputBufferWrapper[K : ClassTag, V : ClassTag](
        val nele : Int,
        val sample : Tuple2[K, V], entryPoint : Entrypoint,
        sparseVectorSizeHandler : Option[Function[Int, Int]],
        denseVectorSizeHandler : Option[Function[Int, Int]],
        val isInput : Boolean, val blockingCopies : Boolean) extends InputBufferWrapper[Tuple2[K, V]] {

  def this(nele : Int, sample : Tuple2[K, V], entryPoint : Entrypoint, blockingCopies : Boolean) =
      this(nele, sample, entryPoint, None, None, true, blockingCopies)
  
  val classModel : ClassModel =
    entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
        new ObjectMatcher(sample))
  val structSize = classModel.getTotalStructSize
  val structMembers : java.util.ArrayList[FieldNameInfo] = classModel.getStructMembers
  assert(structMembers.size == 2)
  assert(structMembers.get(0).name.equals("_1") ||
      structMembers.get(1).name.equals("_1"))

  val desc0 = structMembers.get(0).desc
  val desc1 = structMembers.get(1).desc
  val size0 = entryPoint.getSizeOf(desc0)
  val size1 = entryPoint.getSizeOf(desc1)

  val zeroIsfirst = structMembers.get(0).name.equals("_1")
  val firstMemberDesc = if (zeroIsfirst) desc0 else desc1
  val secondMemberDesc = if (zeroIsfirst) desc1 else desc0
  val firstMemberSize = if (zeroIsfirst) size0 else size1
  val secondMemberSize = if (zeroIsfirst) size1 else size0

  val firstMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._1.getClass.getName,
                new NameMatcher(sample._1.getClass.getName))
  val secondMemberClassModel : ClassModel =
        entryPoint.getModelFromObjectArrayFieldsClasses(
                sample._2.getClass.getName,
                new NameMatcher(sample._2.getClass.getName))

  var used : Int = -1

  val buffer1 = OpenCLBridgeWrapper.getInputBufferFor[K](nele,
          entryPoint, sample._1.getClass.getName, sparseVectorSizeHandler,
          denseVectorSizeHandler, DenseVectorInputBufferWrapperConfig.tiling, blockingCopies)
  val buffer2 = OpenCLBridgeWrapper.getInputBufferFor[V](nele,
          entryPoint, sample._2.getClass.getName, sparseVectorSizeHandler,
          denseVectorSizeHandler, DenseVectorInputBufferWrapperConfig.tiling, blockingCopies)

  def getObjFieldOffsets(desc : String, classModel : ClassModel) : Array[Long] = {
    desc match {
      case "I" => { Array(OpenCLBridgeWrapper.intValueOffset) }
      case "F" => { Array(OpenCLBridgeWrapper.floatValueOffset) }
      case "D" => { Array(OpenCLBridgeWrapper.doubleValueOffset) }
      case _ => {
        classModel.getStructMemberOffsets
      }
    }
  }

  def getObjFieldSizes(desc : String, classModel : ClassModel) : Array[Int] = {
    desc match {
      case "I" => { Array(4) }
      case "F" => { Array(4) }
      case "D" => { Array(8) }
      case _ => {
        classModel.getStructMemberSizes
      }
    }
  }

  def getTotalSize(desc : String, classMOdel : ClassModel) : Int = {
    desc match {
      case "I" => { 4 }
      case "F" => { 4 }
      case "D" => { 8 }
      case _ => {
        classModel.getTotalStructSize
      }
    }
  }

  override def flush() {
    buffer1.flush
    buffer2.flush
  }

  override def append(obj : Any) {
    append(obj.asInstanceOf[Tuple2[K, V]])
  }

  def append(obj : Tuple2[K, V]) {
    if (firstMemberSize > 0) {
      buffer1.append(obj._1)
    }
    if (secondMemberSize > 0) {
      buffer2.append(obj._2)
    }
  }

  override def aggregateFrom(iter : Iterator[Tuple2[K, V]]) {
    if (firstMemberSize > 0 && secondMemberSize > 0) {
      while (!buffer1.outOfSpace && !buffer2.outOfSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        buffer1.append(obj._1)
        buffer2.append(obj._2)
      }

    } else if (firstMemberSize > 0) {
      while (!buffer1.outOfSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        buffer1.append(obj._1)
      }

    } else if (secondMemberSize > 0) {
      while (!buffer2.outOfSpace && iter.hasNext) {
        val obj : Tuple2[K, V] = iter.next
        buffer2.append(obj._2)
      }
    }
  }

  override def nBuffered() : Int = {
    val buffered1 = buffer1.nBuffered
    val buffered2 = buffer2.nBuffered
    if (buffered1 < buffered2) buffered1 else buffered2
  }

  override def copyToDevice(startArgnum : Int, ctx : Long, dev_ctx : Long,
          cacheId : CLCacheID, persistent : Boolean, limit : Int = -1) : Int = {
    val firstMemberUsed = if (firstMemberSize > 0) buffer1.countArgumentsUsed
        else 1
    val secondMemberUsed = if (secondMemberSize > 0) buffer2.countArgumentsUsed
        else 1
    assert(limit == -1)

    // Find least number of elements buffered
    var tocopy : Int = -1
    if (firstMemberSize > 0 && secondMemberSize > 0) {
        tocopy = if (buffer1.nBuffered < buffer2.nBuffered) buffer1.nBuffered
            else buffer2.nBuffered
    } else if (firstMemberSize > 0) {
        tocopy = buffer1.nBuffered
    } else if (secondMemberSize > 0) {
        tocopy = buffer2.nBuffered
    }

    if (firstMemberSize > 0) {
        buffer1.copyToDevice(startArgnum, ctx, dev_ctx, cacheId, persistent,
                limit=tocopy)
        cacheId.incrComponent(firstMemberUsed)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum)
    }

    if (secondMemberSize > 0) {
        buffer2.copyToDevice(startArgnum + firstMemberUsed, ctx, dev_ctx,
                cacheId, persistent, limit=tocopy)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum + firstMemberUsed)
    }

    used = tocopy

    if (isInput) {
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx,
              startArgnum + firstMemberUsed + secondMemberUsed,
              structSize * tocopy, persistent)
      return firstMemberUsed + secondMemberUsed + 1
    } else {
      return firstMemberUsed + secondMemberUsed
    }
  }

  override def countArgumentsUsed : Int = {
    val firstMemberUsed = if (firstMemberSize > 0) buffer1.countArgumentsUsed
        else 1
    val secondMemberUsed = if (secondMemberSize > 0) buffer2.countArgumentsUsed
        else 1
    if (isInput) firstMemberUsed + secondMemberUsed + 1
        else firstMemberUsed + secondMemberUsed
  }

  override def hasNext() : Boolean = {
    buffer1.hasNext && buffer2.hasNext
  }

  override def next() : Tuple2[K, V] = {
    (buffer1.next, buffer2.next)
  }

  override def haveUnprocessedInputs : Boolean = {
    buffer1.haveUnprocessedInputs || buffer2.haveUnprocessedInputs
  }

  override def outOfSpace : Boolean = {
    buffer1.outOfSpace || buffer2.outOfSpace
  }

  override def releaseNativeArrays {
    buffer1.releaseNativeArrays
    buffer2.releaseNativeArrays
  }

  override def reset() {
    buffer1.reset
    buffer2.reset
  }

  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint, persistent : Boolean) : Int = {
    val firstMemberClassName : String = CodeGenUtil.cleanClassName(
            sample._1.getClass.getName, objectMangling = true)
    val secondMemberClassName : String = CodeGenUtil.cleanClassName(
            sample._2.getClass.getName, objectMangling = true)
    var usedArgs = 0

    val firstMemberSize = entryPoint.getSizeOf(firstMemberClassName)
    val secondMemberSize = entryPoint.getSizeOf(secondMemberClassName)

    val nLoaded : Int = OpenCLBridge.fetchNLoaded(id.rdd, id.partition, id.offset)
    if (nLoaded == -1) return -1

    if (firstMemberSize > 0) {
        val cacheSuccess = RuntimeUtil.tryCacheHelper(firstMemberClassName, ctx,
                dev_ctx, 0, id, nLoaded,
                DenseVectorInputBufferWrapperConfig.tiling, entryPoint, persistent)
        if (cacheSuccess == -1) {
          return -1
        }
        usedArgs = usedArgs + cacheSuccess
        id.incrComponent(usedArgs)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, 0)
        usedArgs = usedArgs + 1
    }

    if (secondMemberSize > 0) {
        val cacheSuccess = RuntimeUtil.tryCacheHelper(secondMemberClassName,
                ctx, dev_ctx, 0 + usedArgs, id, nLoaded,
                DenseVectorInputBufferWrapperConfig.tiling,
                entryPoint, persistent)
        if (cacheSuccess == -1) {
          OpenCLBridge.manuallyRelease(ctx, dev_ctx, 0, usedArgs)
          return -1
        }
        usedArgs = usedArgs + cacheSuccess
    } else {
        OpenCLBridge.setNullArrayArg(ctx, 0 + usedArgs)
        usedArgs = usedArgs + 1
    }

    val tuple2ClassModel : ClassModel =
      entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
          new ObjectMatcher(sample))
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, 0 + usedArgs,
            tuple2ClassModel.getTotalStructSize * nLoaded, false)
    return usedArgs + 1
  }
}
