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

    val partitionDeviceHint : Int = OpenCLBridge.getDeviceHintFor(
            firstParent[T].id, split.index, 0, 0)

//    val deviceInitStart = System.currentTimeMillis // PROFILE
    val device_index = OpenCLBridge.getDeviceToUse(partitionDeviceHint,
            threadId, CLConfig.heapsPerDevice,
            CLConfig.heapSize, CLConfig.percHighPerfBuffers,
            false)
//    System.err.println("Thread " + threadId + " selected device " + device_index) // PROFILE
    val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index,
            CLConfig.heapsPerDevice, CLConfig.heapSize,
            CLConfig.percHighPerfBuffers, false)
    val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)
//    RuntimeUtil.profPrint("DeviceInit", deviceInitStart, threadId) // PROFILE

    val outStream : AsyncOutputStream[U] = new AsyncOutputStream[U](multiOutput, dev_ctx, threadId)

    val runner : Runnable = new Runnable {
      override def run() {
        for (v <- nested) {
          try {
            f(v, outStream)
          } catch {
            case suspend : SuspendException => ;
            case e : Exception => throw e
          }
        }

        outStream.finish
      }
    }
    val thread : Thread = new Thread(runner)
    thread.start

    new Iterator[U] {
      var nex : Option[U] = outStream.pop

      override def hasNext() : Boolean = {
        !nex.isEmpty
      }

      override def next() : U = {
        if (nex.isEmpty) throw new RuntimeException("next called when none left")

        val n : U = nex.get
        nex = outStream.pop
        n
      }
    }
  }
}
