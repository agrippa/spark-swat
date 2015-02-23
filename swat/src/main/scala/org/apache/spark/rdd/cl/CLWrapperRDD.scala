package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLWrapperRDD[U: ClassTag](prev: RDD[U])
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[U].partitions

  override def compute(split: Partition, context: TaskContext) = {
    // Do nothing
    val iter = new Iterator[U] {
      val nested = firstParent[U].iterator(split, context)

      def hasNext : Boolean = {
        nested.hasNext
      }

      def next : U = {
        nested.next
      }
    }
    iter
  }

  override def map[T: ClassTag](f: U => T): RDD[T] = {
    new CLMappedRDD(this, sparkContext.clean(f))
  }
}

object CLWrapper {
  def cl[T: ClassTag](rdd : RDD[T]) : CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd)
  }
}
