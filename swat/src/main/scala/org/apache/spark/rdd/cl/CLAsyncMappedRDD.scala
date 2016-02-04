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

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

import java.net._
import java.util.LinkedList
import java.util.Map
import java.util.HashMap

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.SparseVectorClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel
import com.amd.aparapi.internal.writer.BlockWriter
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.writer.ScalaParameter.DIRECTION

class CLAsyncMappedRDD[U: ClassTag, T: ClassTag, M: ClassTag](val prev: RDD[T],
    val f: (T, AsyncOutputStream[U, M]) => Unit, val useSwat : Boolean,
    val multiOutput : Boolean) extends RDD[Tuple2[U, Option[M]]](prev) {

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[Tuple2[U, Option[M]]] = {
    val nested = firstParent[T].iterator(split, context)
    val threadId : Int = RuntimeUtil.getThreadID

    if (!useSwat) {
      val outStream : JVMAsyncOutputStream[U, M] =
          new JVMAsyncOutputStream[U, M](!multiOutput)

      return new Iterator[Tuple2[U, Option[M]]] {
        def next() : Tuple2[U, Option[M]] = {
          if (outStream.isEmpty) {
            var caughtException : Boolean = false
            try {
              f(nested.next, outStream)
            } catch {
              case s : SuspendException => caughtException = true
              case e : Exception => throw e
            }
            if (!multiOutput) assert(caughtException)
            // requires that each lambda call produces at least one output
            assert(!outStream.isEmpty)
          }
          outStream.pop.get
        }

        def hasNext() : Boolean = {
          nested.hasNext || !outStream.isEmpty
        }
      }
    } else {
      val evaluator = (lambda : Function0[U]) => lambda()
      val outputStream = new CLAsyncOutputStream[U, M](!multiOutput)
      val nestedWrapper : Iterator[Function0[U]] = new Iterator[Function0[U]] {

        override def next() : Function0[U] = {
          if (outputStream.lambdas.isEmpty) {
            var caughtException : Boolean = false
            try {
              f(nested.next, outputStream)
            } catch {
              case s: SuspendException => caughtException = true
              case e: Throwable => throw e
            }
            if (!multiOutput) assert(caughtException)
            assert(!outputStream.lambdas.isEmpty)
          }

          return outputStream.lambdas.poll()
        }

        override def hasNext() : Boolean = {
          nested.hasNext || !outputStream.lambdas.isEmpty
        }
      }

      val clIter = new PullCLRDDProcessor(nestedWrapper, evaluator, context,
              firstParent[T].id, split.index)

      return new Iterator[Tuple2[U, Option[M]]] {
        override def next() : Tuple2[U, Option[M]] = {
          (clIter.next, outputStream.metadata.poll)
        }
        override def hasNext() : Boolean = { clIter.hasNext }
      }
    }
  }
}
