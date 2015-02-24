package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val iter = new Iterator[U] {
      val nested = firstParent[T].iterator(split, context)

      var index = 0
      var nLoaded = 0
      val N = 1024
      val acc : Array[T] = new Array[T](N)

      def next() : U = {
        if (index >= nLoaded) {
          assert(nested.hasNext)
          System.err.println("Reading in " + N + " inputs")

          index = 0
          nLoaded = 0
          while (nLoaded < N && nested.hasNext) {
            acc(nLoaded) = nested.next
            nLoaded = nLoaded + 1
          }
        }

        val curr = index
        index = index + 1
        f(acc(curr))
      }

      def hasNext : Boolean = {
        (index < nLoaded || nested.hasNext)
      }
    }
    iter
  }
}
