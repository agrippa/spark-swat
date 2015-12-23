package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.BufferOverflowException
import java.nio.ByteOrder
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldNameInfo
import com.amd.aparapi.internal.util.UnsafeWrapper

import java.nio.ByteBuffer

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

class LambdaInputBufferWrapper[L: ClassTag](val nele : Int, val sample : L,
        val lambdaEntrypoint : Entrypoint,
        val sparseVectorSizeHandler : Option[Function[Int, Int]],
        val denseVectorSizeHandler : Option[Function[Int, Int]],
        val isInput : Boolean, val blockingCopies : Boolean)
        extends InputBufferWrapper[L] {

  def this(nele : Int, sample : L, entryPoint : Entrypoint,
          blockingCopies : Boolean) =
      this(nele, sample, entryPoint, None, None, true, blockingCopies)

  val lambdaClassModel = lambdaEntrypoint.getClassModel
  val structMembers : java.util.ArrayList[FieldNameInfo] = lambdaClassModel.getStructMembers
  val nReferencedFields = lambdaEntrypoint.getReferencedClassModelFields.size

  val fields = new Array[Field](nReferencedFields)
  var fieldSizes = new Array[Int](nReferencedFields)
  val buffers = new Array[InputBufferWrapper[_]](nReferencedFields)
  var fieldNumArgs = new Array[Int](nReferencedFields)

  var index : Int = 0
  val iter = lambdaEntrypoint.getReferencedClassModelFields.iterator
  while (iter.hasNext) {
    val field = iter.next
    val fieldName = field.getName
    val fieldDesc = field.getDescriptor

    val javaField : Field = sample.getClass.getDeclaredField(fieldName)
    javaField.setAccessible(true)
    val fieldSample : java.lang.Object = javaField.get(sample)

    fields(index) = javaField
    fieldSizes(index) = lambdaEntrypoint.getSizeOf(fieldDesc)
    buffers(index) = OpenCLBridgeWrapper.getInputBufferFor(nele,
            lambdaEntrypoint, fieldSample.getClass.getName,
            sparseVectorSizeHandler, denseVectorSizeHandler,
            DenseVectorInputBufferWrapperConfig.tiling,
            SparseVectorInputBufferWrapperConfig.tiling,
            RuntimeUtil.getElementVectorLengthHint(fieldSample), blockingCopies)
    fieldNumArgs(index) = if (fieldSizes(index) > 0) buffers(index).countArgumentsUsed else 1

    index += 1
  }

  var nativeBuffers : LambdaNativeInputBuffers[L] = null

  override def selfAllocate(dev_ctx : Long) {
    nativeBuffers = generateNativeInputBuffer(dev_ctx).asInstanceOf[LambdaNativeInputBuffers[L]]
  }

  override def getCurrentNativeBuffers : NativeInputBuffers[L] = nativeBuffers
  override def setCurrentNativeBuffers(set : NativeInputBuffers[_]) {
    nativeBuffers = set.asInstanceOf[LambdaNativeInputBuffers[L]]
    if (set == null) {
      for (b <- buffers) {
        b.setCurrentNativeBuffers(null)
      }
    } else {
      for (i <- 0 until nReferencedFields) {
        buffers(i).setCurrentNativeBuffers(nativeBuffers.nativeBuffers(i))
      }
    }
  }

  override def flush() {
    for (b <- buffers) {
      b.flush
    }
  }

  override def setupNativeBuffersForCopy(limit : Int) {
    assert(limit == -1) // No nesting of lambda buffers
    assert(nativeBuffers.tocopy == -1)

    val tocopy : Int = nBuffered()

    nativeBuffers.tocopy = tocopy
    for (b <- buffers) {
      b.setupNativeBuffersForCopy(tocopy)
    }
  }

  override def transferOverflowTo(
          otherAbstract : NativeInputBuffers[_]) :
          NativeInputBuffers[L] = {
    assert(nativeBuffers.tocopy != -1)

    val other : LambdaNativeInputBuffers[L] =
        otherAbstract.asInstanceOf[LambdaNativeInputBuffers[L]]

    for (i <- 0 until nReferencedFields) {
      buffers(i).transferOverflowTo(other.nativeBuffers(i))
    }

    other.tocopy = -1

    val oldBuffers = nativeBuffers
    nativeBuffers = other
    return oldBuffers
  }

  override def append(obj : Any) {
    for (i <- 0 until nReferencedFields) {
      if (fieldSizes(i) > 0) {
        val value = fields(i).get(obj)
        buffers(i).append(value)
      }
    }
  }

  override def aggregateFrom(iter : Iterator[L]) {
    while (!outOfSpace && iter.hasNext) {
      val obj : L = iter.next
      append(obj)
    }
  }

  override def nBuffered() : Int = {
    var minBuffered : Int = -1
    for (i <- 0 until nReferencedFields) {
      if (fieldSizes(i) > 0) {
        if (minBuffered == -1 || buffers(i).nBuffered < minBuffered) {
          minBuffered = buffers(i).nBuffered
        }
      }
    }
    assert(minBuffered != -1)
    minBuffered
  }

  override def countArgumentsUsed : Int = {
    var used : Int = 0
    for (i <- 0 until nReferencedFields) {
      if (fieldSizes(i) > 0) {
        used += buffers(i).countArgumentsUsed
      } else {
        used += 1
      }
    }

    used
  }

  override def haveUnprocessedInputs : Boolean = {
    for (b <- buffers) {
      if (b.haveUnprocessedInputs) {
        return true
      }
    }
    return false
  }

  override def outOfSpace : Boolean = {
    for (b <- buffers) {
      if (b.outOfSpace) {
        return true
      }
    }
    return false
  }

  override def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[L] = {
    //TODO
    new LambdaNativeInputBuffers(nReferencedFields, buffers, fieldSizes,
            fieldNumArgs, dev_ctx)
  }

  override def reset() {
    for (b <- buffers) {
      b.reset
    }
  }

  override def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint, persistent : Boolean) : Int = {
    // A lambda could only be in a global array, and we don't cache those.
    throw new UnsupportedOperationException
  }

}
