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

class CLAsyncMappedRDD[U: ClassTag, T: ClassTag](val prev: RDD[T],
    val f: (T, AsyncOutputStream[U]) => Unit, val useSwat : Boolean,
    val multiOutput : Boolean) extends RDD[U](prev) {

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    val nested = firstParent[T].iterator(split, context)
    val threadId : Int = RuntimeUtil.getThreadID

    if (!useSwat) {
      val outStream : JVMAsyncOutputStream[U] = new JVMAsyncOutputStream(
              !multiOutput)

      return new Iterator[U] {
        def next() : U = {
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
      val nestedWrapper : Iterator[Function0[U]] = new Iterator[Function0[U]] {
        val outputStream = new AsyncOutputStream[U] {
          val lambdas : LinkedList[Function0[U]] = new LinkedList[Function0[U]]

          override def spawn(l : () => U) {
            lambdas.add(l)
            if (!multiOutput) {
              throw new SuspendException
            }
          }

          override def finish() {
            throw new UnsupportedOperationException
          }

          override def pop() : Option[U] = {
            throw new UnsupportedOperationException
          }
        }

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

      return new CLRDDProcessor(nestedWrapper, evaluator, context,
              firstParent[T].id, split.index)
    }
  }
}
