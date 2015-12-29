package org.apache.spark.rdd.cl

import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import scala.reflect.ClassTag

class CLAccelOutputStream[U: ClassTag](context : TaskContext, rddId : Int,
    partitionId : Int) extends AccelOutputStream[U] {

  private var finished : Boolean = false
  private val buffered : LinkedList[() => U] = new LinkedList[() => U]

  val evaluator = (lambda : Function0[U]) => lambda()

  val processor : Iterator[U] = null

  override def map(l : Int => U, N : Int) : Array[U] = {
    throw new UnsupportedOperationException
    // val arr = new Array[U](N)
    // if (processor == null) {
    //   val fakeIter = new Iterator[Int] {
    //     var count : Int = 0
    //     override def next() : Int = {
    //       val result = count
    //       count += 1
    //       result
    //     }
    //     override def hasNext() : Boolean = { count < N }
    //   }
    //   processor = new CLRDDProcessor(fakeIter

  }

  override def markFinished() {
    throw new UnsupportedOperationException
    // buffered.synchronized {
    //   finished = true
    //   buffered.notify
    // }
  }
}
