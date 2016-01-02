package org.apache.spark.rdd.cl

import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import scala.reflect.ClassTag

class CLAccelOutputStream[U: ClassTag](context : TaskContext, rddId : Int,
    partitionId : Int) extends AccelOutputStream[U] {

  var processor : PushCLRDDProcessor[Int, U] = null

  override def map(l : Int => U, N : Int, accel : Boolean) : Array[U] = {
    val arr = new Array[U](N)

    if (accel) {
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
    } else {
      for (i <- 0 until N) {
        arr(i) = l(i)
      }
    }

    arr
  }

  override def markFinished() {
    processor.cleanup
  }
}
