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

class CLAsyncMapPartitionsRDD[U: ClassTag, T: ClassTag, M: ClassTag](
    val prev: RDD[T], val f: (Iterator[T], AsyncOutputStream[U, M]) => Unit,
    val useSwat : Boolean) extends RDD[Tuple2[U, Option[M]]](prev) {

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[Tuple2[U, Option[M]]] = {
    val nested = firstParent[T].iterator(split, context)
    val threadId : Int = RuntimeUtil.getThreadID

    if (!useSwat) {
      val outStream : JVMAsyncOutputStream[U, M] =
          new JVMAsyncOutputStream[U, M](false)

      f(nested, outStream)

      return new Iterator[Tuple2[U, Option[M]]] {
        def next() : Tuple2[U, Option[M]] = { outStream.pop.get }
        def hasNext() : Boolean = { !outStream.isEmpty }
      }
    } else {
      /*
       * TODO this leads to larger memory requirements as a whole partition's
       * output is buffered at a time.
       */
      val evaluator = (lambda : Function0[U]) => lambda()
      val outputStream = new CLAsyncOutputStream[U, M](false)
      f(nested, outputStream)

      val nestedWrapper : Iterator[Function0[U]] = new Iterator[Function0[U]] {
        override def next() : Function0[U] = { outputStream.lambdas.poll() }
        override def hasNext() : Boolean = { !outputStream.lambdas.isEmpty }
      }

      val clIter : Iterator[U] = new PullCLRDDProcessor(nestedWrapper, evaluator,
              context, firstParent[T].id, split.index)

      return new Iterator[Tuple2[U, Option[M]]] {
        override def next() : Tuple2[U, Option[M]] = {
          (clIter.next, outputStream.metadata.poll)
        }
        override def hasNext() : Boolean = { clIter.hasNext }
      }
    }
  }
}
