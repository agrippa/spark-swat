package org.apache.spark.rdd.cl

import java.util.LinkedList

import scala.reflect.ClassTag

class CLAsyncOutputStream[U: ClassTag](val singleInstance : Boolean)
    extends AsyncOutputStream[U] {
  val lambdas : LinkedList[Function0[U]] = new LinkedList[Function0[U]]

  override def spawn(l : () => U) {
    lambdas.add(l)
    if (singleInstance) {
      throw new SuspendException
    }
  }

  override def finish() {
    throw new UnsupportedOperationException
  }

  override def pop() : Option[U] = {
    throw new UnsupportedOperationException
  }
}
