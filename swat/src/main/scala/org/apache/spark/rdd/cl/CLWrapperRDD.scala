/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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

  def mapAsync[U: ClassTag, M: ClassTag](
          f: (T, AsyncOutputStream[U, M]) => Unit) : RDD[Tuple2[U, Option[M]]] = {
    new CLAsyncMappedRDD(prev, sparkContext.clean(f), useSwat, false)
  }

  def flatMapAsync[U: ClassTag, M: ClassTag](
          f: (T, AsyncOutputStream[U, M]) => Unit) : RDD[Tuple2[U, Option[M]]] = {
    new CLAsyncMappedRDD(prev, sparkContext.clean(f), useSwat, true)
  }

  def mapPartitionsAsync[U: ClassTag, M: ClassTag](
      f : (Iterator[T], AsyncOutputStream[U, M]) => Unit) : RDD[Tuple2[U, Option[M]]] = {
    new CLAsyncMapPartitionsRDD(prev, sparkContext.clean(f), useSwat)
  }

  def mapPartitionsAccel[U: ClassTag, M: ClassTag](
      f : (Iterator[T], AccelOutputStream[M]) => Iterator[U]) : RDD[U] = {
    new CLAccelMapPartitionsRDD(prev, sparkContext.clean(f), useSwat)
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
