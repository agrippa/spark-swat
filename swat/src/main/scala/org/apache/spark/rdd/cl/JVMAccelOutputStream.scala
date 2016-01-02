package org.apache.spark.rdd.cl

import java.util.LinkedList
import java.lang.reflect.Field

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.writer.ScalaArrayParameter

import java.util.HashMap

class JVMAccelOutputStream[U: ClassTag] extends AccelOutputStream[U] {

  val buffered : LinkedList[U] = new LinkedList[U]

  override def map(l : Int => U, N : Int, accel : Boolean = false) : Array[U] = {
    val arr = new Array[U](N)
    for (i <- 0 until N) {
      arr(i) = l(i)
    }
    arr
  }

  override def markFinished() {
    throw new UnsupportedOperationException
  }
}
