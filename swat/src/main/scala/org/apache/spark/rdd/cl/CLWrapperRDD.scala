package org.apache.spark.rdd.cl

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.Logging

class CLWrapperPairRDD[K : ClassTag, V : ClassTag](self : RDD[Tuple2[K, V]],
    useSwat : Boolean) extends Logging with Serializable {
  def mapValues[U : ClassTag](f : V => U) : RDD[Tuple2[K, U]] = {
    new CLMappedValuesRDD[K, V, U](self, self.context.clean(f), useSwat)
  }

  def map[U: ClassTag](f: Tuple2[K, V] => U) : RDD[U] = {
    new CLMappedRDD(self, self.context.clean(f), useSwat)
  }
}

class CLWrapperRDD[T: ClassTag](val prev: RDD[T], val useSwat : Boolean)
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
    new CLMappedRDD(prev, sparkContext.clean(f), useSwat)
  }

  def mapAsync[U: ClassTag](f: (T, AsyncOutputStream[U]) => Unit) :
      RDD[U] = {
    new CLAsyncMappedRDD(prev, sparkContext.clean(f), useSwat, false)
  }

  def flatMapAsync[U: ClassTag](f: (T, AsyncOutputStream[U]) => Unit) :
      RDD[U] = {
    new CLAsyncMappedRDD(prev, sparkContext.clean(f), useSwat, true)
  }
}

object CLWrapper {
  def cl[T: ClassTag](rdd : RDD[T], useSwat : Boolean = true) :
      CLWrapperRDD[T] = {
    new CLWrapperRDD[T](rdd, useSwat)
  }

  def pairCl[K : ClassTag, V : ClassTag](rdd : RDD[Tuple2[K, V]],
      useSwat : Boolean = true) : CLWrapperPairRDD[K, V] = {
    new CLWrapperPairRDD[K, V](rdd, useSwat)
  }
}
