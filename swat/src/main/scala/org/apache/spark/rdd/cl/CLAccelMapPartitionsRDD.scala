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

class CLAccelMapPartitionsRDD[U: ClassTag, T: ClassTag, M: ClassTag](
    val prev: RDD[T], val f: (Iterator[T], AccelOutputStream[M]) => Iterator[U],
    val useSwat : Boolean) extends RDD[U](prev) {

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    val nested = firstParent[T].iterator(split, context)
    val threadId : Int = RuntimeUtil.getThreadID

    if (!useSwat) {
      val outStream : JVMAccelOutputStream[M] = new JVMAccelOutputStream[M]

      f(nested, outStream)
    } else {
      val outputStream = new CLAccelOutputStream[M](context, firstParent[T].id, split.index)
      val iter = f(nested, outputStream)
      new Iterator[U] {
        override def next() : U = { iter.next }
        override def hasNext() : Boolean = {
          val haveNext = iter.hasNext
          if (!haveNext) outputStream.markFinished
          haveNext
        }
      }
    }
  }
}
