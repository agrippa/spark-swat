package org.apache.spark.rdd.cl

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLWrapperRDD[T: ClassTag](prev: RDD[T], enableNested : Boolean)
    extends RDD[T](prev) {
  def this(prev : RDD[T]) = this(prev, true)

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
  val counter : AtomicInteger = new AtomicInteger(0)

  def cl[T: ClassTag](rdd : RDD[T]) : CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd)
  }

  def cl[T: ClassTag](rdd : RDD[T], enableNested : Boolean) : CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd, enableNested)
  }
}
