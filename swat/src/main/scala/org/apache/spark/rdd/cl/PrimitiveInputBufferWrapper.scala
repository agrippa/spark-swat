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

class PrimitiveInputBufferWrapper[T: ClassTag](val N : Int) extends InputBufferWrapper[T]{
  val arr : Array[T] = new Array[T](N)
  var filled : Int = 0

  override def hasSpace() : Boolean = {
    filled < arr.length
  }

  override def append(obj : Any) {
    arr(filled) = obj.asInstanceOf[T]
    filled += 1
  }

  override def aggregateFrom(iter : Iterator[T]) : Int = {
    val startFilled = filled;
    while (hasSpace && iter.hasNext) {
        arr(filled) = iter.next
        filled += 1
    }
    filled - startFilled
  }

  override def flush() { }

  override def copyToDevice(argnum : Int, ctx : Long, dev_ctx : Long,
      broadcastid : Int, rddid : Int, partitionid : Int, offset : Int,
      component : Int) : Int = {
    if (arr.isInstanceOf[Array[Double]]) {
      OpenCLBridge.setDoubleArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Double]], filled, broadcastid, rddid,
          partitionid, offset, component)
    } else if (arr.isInstanceOf[Array[Int]]) {
      OpenCLBridge.setIntArrayArg(ctx, dev_ctx, argnum,
              arr.asInstanceOf[Array[Int]], filled, broadcastid, rddid,
              partitionid, offset, component)
    } else if (arr.isInstanceOf[Array[Float]]) {
      OpenCLBridge.setFloatArrayArg(ctx, dev_ctx, argnum,
          arr.asInstanceOf[Array[Float]], filled, broadcastid, rddid,
          partitionid, offset, component)
    } else {
      throw new RuntimeException("Unsupported")
    }

    filled = 0
    return 1
  }
}
