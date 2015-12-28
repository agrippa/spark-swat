package org.apache.spark.rdd.cl

import java.util.LinkedList

import scala.reflect.ClassTag

class CLAsyncOutputStream[U: ClassTag, M: ClassTag](val singleInstance : Boolean)
    extends AsyncOutputStream[U, M] {
  val lambdas : LinkedList[Function0[U]] = new LinkedList[Function0[U]]
  val metadata : LinkedList[Option[M]] = new LinkedList[Option[M]]

  override def spawn(l : () => U, m : Option[M]) {
    lambdas.add(l)
    metadata.add(m)

    if (singleInstance) {
      throw new SuspendException
    }
  }

  override def pop() : Option[Tuple2[U, Option[M]]] = {
    throw new UnsupportedOperationException
  }
}
