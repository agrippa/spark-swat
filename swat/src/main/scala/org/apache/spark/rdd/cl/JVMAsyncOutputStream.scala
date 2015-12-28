package org.apache.spark.rdd.cl

import java.util.LinkedList
import java.lang.reflect.Field

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.writer.ScalaArrayParameter

class JVMAsyncOutputStream[U: ClassTag, M: ClassTag](val singleInstance : Boolean)
    extends AsyncOutputStream[U, M] {

  val buffered : LinkedList[U] = new LinkedList[U]
  val metadataBuffered : LinkedList[Option[M]] = new LinkedList[Option[M]]

  override def spawn(l : () => U, metadata : Option[M]) {
    val value = l()
    assert(value != null)

    buffered.add(value)
    metadataBuffered.add(metadata)

    if (singleInstance) {
      throw new SuspendException
    }
  }

  def pop() : Option[Tuple2[U, Option[M]]] = {
    var result : Option[U] = None

    if (buffered.isEmpty) {
      None
    } else {
      Some((buffered.poll, metadataBuffered.poll))
    }
  }

  def isEmpty() : Boolean = buffered.isEmpty
}
