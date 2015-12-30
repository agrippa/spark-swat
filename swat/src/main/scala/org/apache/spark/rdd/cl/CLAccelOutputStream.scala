package org.apache.spark.rdd.cl

import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import scala.reflect.ClassTag

class CLAccelOutputStream[U: ClassTag](context : TaskContext, rddId : Int,
    partitionId : Int) extends AccelOutputStream[U] {

  private var finished : Boolean = false
  private val buffered : LinkedList[() => U] = new LinkedList[() => U]

  val evaluator = (lambda : Function0[U]) => lambda()

  var processor : PushCLRDDProcessor[Int, U] = null

  override def map(l : Int => U, N : Int) : Array[U] = {
    val arr = new Array[U](N)
    // TODO avoid creating new processor every time, just don't cache the lambda-captured arguments
    if (processor == null) {
      processor = new PushCLRDDProcessor(0, l, context, rddId, partitionId)
    } else {
      processor.resetGlobalArguments(l)
    }
    
    for (i <- 0 until N) {
      processor.push(i)
    }
    processor.flush

    for (i <- 0 until N) {
      arr(i) = processor.next
    }

    arr
  }

  override def markFinished() {
    processor.cleanup
  }
}
