package org.apache.spark.rdd.cl

import java.util.concurrent.ConcurrentLinkedQueue

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLMappedValuesRDD[K: ClassTag, V: ClassTag, U : ClassTag](val prev: RDD[Tuple2[K, V]],
    val f: V => U) extends RDD[Tuple2[K, U]](prev) {
  override def getPartitions: Array[Partition] = firstParent[Tuple2[K, V]].partitions

  override def compute(split: Partition, context: TaskContext) :
      Iterator[Tuple2[K, U]] = {
    return new Iterator[Tuple2[K, U]] {
      val nested = firstParent[Tuple2[K, V]].iterator(split, context)

      /*
       * If bufferedKeysIter == bufferedKeysLimit && bufferedKeysFull, then the
       * buffer is full. Otherwise, it is empty.
       */
      val bufferedKeys : ConcurrentLinkedQueue[K] = new ConcurrentLinkedQueue[K]()

      /*
       * These next and hasNext methods will be called by a separate thread, so
       * they must be thread-safe with respect to this thread.
       */
      val valueIter : Iterator[V] = new Iterator[V] {
        override def next() : V = {
          val n : Tuple2[K, V] = nested.next
          bufferedKeys.add(n._1)
          n._2
        }
        override def hasNext() : Boolean = {
          nested.hasNext
        }
      }

      val valueProcessor : CLRDDProcessor[V, U] = new CLRDDProcessor[V, U](
              valueIter, f, context, firstParent[Tuple2[K, V]].id, split.index)

      def next() : Tuple2[K, U] = {
       val nextVal : U = valueProcessor.next
       val nextKey : K = bufferedKeys.poll
       assert(nextKey != null)
       (nextKey, nextVal)
      }

      def hasNext() : Boolean = {
        valueProcessor.hasNext
      }
    }
  }
}
