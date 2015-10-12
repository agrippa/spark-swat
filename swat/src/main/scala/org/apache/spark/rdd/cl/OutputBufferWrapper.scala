package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.lang.reflect.Constructor
import java.lang.reflect.Field

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.ClassModel.FieldDescriptor
import com.amd.aparapi.internal.util.UnsafeWrapper

trait OutputBufferWrapper[T] {
  def next() : T
  def hasNext() : Boolean

  def countArgumentsUsed() : Int
  /*
   * Called after we have finished with the output buffer for the current inputs
   * to prepare it for future buffering.
   */
  def fillFrom(kernel_ctx : Long, outArgNum : Int)

  def getNativeOutputBufferInfo() : Array[Int]
}
