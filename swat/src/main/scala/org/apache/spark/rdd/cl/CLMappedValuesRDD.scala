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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

class CLMappedValuesRDD[K: ClassTag, V: ClassTag, U : ClassTag](val prev: RDD[Tuple2[K, V]],
    val f: V => U, val useSwat : Boolean) extends RDD[Tuple2[K, U]](prev) {
  override def getPartitions: Array[Partition] = firstParent[Tuple2[K, V]].partitions

  override def compute(split: Partition, context: TaskContext) :
      Iterator[Tuple2[K, U]] = {
    val nested = firstParent[Tuple2[K, V]].iterator(split, context)
    if (useSwat) {
      /*
       * If bufferedKeysIter == bufferedKeysLimit && bufferedKeysFull, then the
       * buffer is full. Otherwise, it is empty.
       */
      val bufferedKeys : ConcurrentLinkedQueue[K] = new ConcurrentLinkedQueue[K]

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

      val valueProcessor : CLRDDProcessor[V, U] = new PullCLRDDProcessor[V, U](
              valueIter, f, context, firstParent[Tuple2[K, V]].id, split.index)

      return new Iterator[Tuple2[K, U]] {
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
    } else {
      return new Iterator[Tuple2[K, U]] {
        def next() : Tuple2[K, U] = {
          val v : Tuple2[K, V] = nested.next
          (v._1, f(v._2))
        }

        def hasNext() : Boolean = {
          nested.hasNext
        }
      }
    }
  }
}
