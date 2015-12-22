package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

class AsyncOutputStream[U: ClassTag](val multiOutput : Boolean) {
  val tmpBuffer : java.util.LinkedList[Option[U]] = new java.util.LinkedList[Option[U]]

  def spawn(l: () => U) {
    assert(l != null)
    val output : Option[U] = Some(l())

    tmpBuffer.synchronized {
      tmpBuffer.add(output)
      tmpBuffer.notify
    }

    if (!multiOutput) {
      throw new SuspendException
    }
  }

  def finish() {
    tmpBuffer.synchronized {
      tmpBuffer.add(None)
      tmpBuffer.notify
    }
  }

  def pop() : Option[U] = {
    var result : Option[U] = None

    tmpBuffer.synchronized {
      while (tmpBuffer.isEmpty) {
        tmpBuffer.wait
      }

      result = tmpBuffer.poll
    }

    result
  }
}
