package org.apache.spark.rdd.cl

import java.util.LinkedList
import java.lang.reflect.Field

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.writer.ScalaArrayParameter

class JVMAsyncOutputStream[U: ClassTag](val singleInstance : Boolean)
    extends AsyncOutputStream[U] {

  val buffered : LinkedList[U] = new LinkedList[U]

  override def spawn(l : () => U) {
    val value = l()
    assert(value != null)

    buffered.synchronized {
      buffered.add(value)
      buffered.notify
    }

    if (singleInstance) {
      throw new SuspendException
    }
  }

  override def finish() {
    throw new UnsupportedOperationException
  }

  def pop() : Option[U] = {
    var result : Option[U] = None

    buffered.synchronized {
      while (buffered.isEmpty) {
        buffered.wait
      }

      result = Some(buffered.poll)
    }
    result
  }
}
