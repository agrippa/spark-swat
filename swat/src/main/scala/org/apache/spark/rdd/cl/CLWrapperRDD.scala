package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLWrapperRDD[T: ClassTag](prev: RDD[T])
    extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    // Do nothing
    val iter = new Iterator[T] {
      val nested = firstParent[T].iterator(split, context)

      def hasNext : Boolean = {
        nested.hasNext
      }

      def next : T = {
        nested.next
      }
    }
    iter
  }

  override def map[U: ClassTag](f: T => U): RDD[U] = {
    new CLMappedRDD(this, sparkContext.clean(f))
  }
}

object CLWrapper {
  def cl[T: ClassTag](rdd : RDD[T]) : CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd)
  }
}
