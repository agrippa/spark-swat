package org.apache.spark.rdd.cl

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLWrapperRDD[T: ClassTag](val prev: RDD[T])
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

  override def map[U: ClassTag](f: T => U) : RDD[U] = {
    new CLMappedRDD(prev, sparkContext.clean(f))
  }
}

object CLWrapper {
  val counter : AtomicInteger = new AtomicInteger(0)

  def cl[T: ClassTag](rdd : RDD[T]) : CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd)
  }
}
