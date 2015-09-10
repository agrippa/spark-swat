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

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
    extends RDD[U](prev) {
  val heapSize = 100 * 1024 * 1024
  var entryPoint : Entrypoint = null
  var openCL : String = null
  val ctxCache : java.util.Map[Long, Long] = new java.util.HashMap[Long, Long]()

  override val partitioner = None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  def profPrint(lbl : String, startTime : Long, threadId : Int) { // PROFILE
      System.err.println("SWAT PROF " + threadId + " " + lbl + " " + // PROFILE
          (System.currentTimeMillis - startTime) + " ms") // PROFILE
  } // PROFILE

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    val N = 65536 * 8
    var acc : Option[InputBufferWrapper[T]] = None
    var outputBuffer : Option[OutputBufferWrapper[U]] = None
    val bbCache : ByteBufferCache = new ByteBufferCache(2)

    val threadId : Int = RuntimeUtil.getThreadID()

    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    System.setProperty("com.amd.aparapi.enable.ATHROW", "true");

    val classModel : ClassModel = ClassModel.createClassModel(f.getClass, null,
        new ShouldNotCallMatcher())
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    // 1 argument expected for maps
    val params : LinkedList[ScalaArrayParameter] =
        CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, 1)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

    val iter = new Iterator[U] {
      val nested = firstParent[T].iterator(split, context)
      var sampleOutput : java.lang.Object = None

      var totalNLoaded = 0
      val overallStart = System.currentTimeMillis

      def next() : U = {

        if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
          assert(nested.hasNext)

          if (!outputBuffer.isEmpty) {
            outputBuffer.get.releaseBuffers(bbCache)
          }

          val deviceHint : Int = OpenCLBridge.getDeviceHintFor(
                  firstParent[T].id, split.index, totalNLoaded, 0)

          val firstSample : T = nested.next

          val initStart = System.currentTimeMillis // PROFILE
          val device_index = OpenCLBridge.getDeviceToUse(deviceHint, threadId)
          System.err.println("Selected device " + device_index) // PROFILE
          val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index)
          val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)

          if (entryPoint == null) {
            sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
            val entrypointAndKernel : Tuple2[Entrypoint, String] =
                RuntimeUtil.getEntrypointAndKernel[T, U](firstSample, sampleOutput,
                params, f, classModel, descriptor, dev_ctx)
            entryPoint = entrypointAndKernel._1
            openCL = entrypointAndKernel._2

            acc = Some(RuntimeUtil.getInputBufferFor(firstSample, N, entryPoint))
          }

          if (!ctxCache.containsKey(dev_ctx)) {
            ctxCache.put(dev_ctx, OpenCLBridge.createSwatContext(
                        f.getClass.getName, openCL, dev_ctx, threadId,
                        entryPoint.requiresDoublePragma,
                        entryPoint.requiresHeap))
          }
          val ctx : Long = ctxCache.get(dev_ctx)
                
          profPrint("Initialization", initStart, threadId) // PROFILE

          val ioStart : Long = System.currentTimeMillis // PROFILE
          acc.get.append(firstSample)
          val nLoaded = 1 + acc.get.aggregateFrom(nested)
          val myOffset : Int = totalNLoaded
          totalNLoaded += nLoaded

          profPrint("Input-I/O", ioStart, threadId) // PROFILE
          System.err.println("SWAT PROF " + threadId + " Loaded " + nLoaded) // PROFILE

          val writeStart = System.currentTimeMillis // PROFILE

          try {
            var argnum : Int = acc.get.copyToDevice(0, ctx, dev_ctx,
                    -1, if (firstParent[T].getStorageLevel.useMemory)
                        firstParent[T].id else -1, split.index, myOffset, 0)

            val outArgNum : Int = argnum
            argnum += OpenCLBridgeWrapper.setUnitializedArrayArg[U](ctx,
                dev_ctx, argnum, nLoaded, classTag[U].runtimeClass,
                entryPoint, sampleOutput.asInstanceOf[U])

            val iter = entryPoint.getReferencedClassModelFields.iterator
            while (iter.hasNext) {
              val field = iter.next
              val isBroadcast = entryPoint.isBroadcastField(field)
              argnum += OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, argnum, f,
                  field.getName, field.getDescriptor, entryPoint, isBroadcast, bbCache)
            }

            val heapArgStart : Int = argnum
            if (entryPoint.requiresHeap) {
              argnum += OpenCLBridge.createHeap(ctx, dev_ctx, argnum, heapSize,
                      nLoaded)
            }   

            OpenCLBridge.setIntArg(ctx, argnum, nLoaded)
            val anyFailedArgNum = argnum - 1

            profPrint("Write", writeStart, threadId) // PROFILE
            val runStart = System.currentTimeMillis // PROFILE
           
            var complete : Boolean = true
            outputBuffer = Some(OpenCLBridgeWrapper.getOutputBufferFor[U](
                        sampleOutput.asInstanceOf[U], outArgNum, nLoaded, bbCache,
                        entryPoint))
            do {
              OpenCLBridge.run(ctx, dev_ctx, nLoaded, false)
              if (entryPoint.requiresHeap) {
                complete = outputBuffer.get.kernelAttemptCallback(
                        nLoaded, anyFailedArgNum, heapArgStart + 3, outArgNum,
                        heapArgStart, heapSize, ctx, dev_ctx, entryPoint, bbCache,
                        devicePointerSize)
                OpenCLBridge.resetHeap(ctx, dev_ctx, heapArgStart)
              }
            } while (!complete)

            outputBuffer.get.finish(ctx, dev_ctx)

            profPrint("Run", runStart, threadId) // PROFILE
            val readStart = System.currentTimeMillis // PROFILE

            OpenCLBridge.postKernelCleanup(ctx);
            profPrint("Read", readStart, threadId) // PROFILE
          } catch {
            case oom : OpenCLOutOfMemoryException => {
              System.err.println("SWAT PROF " + threadId + " OOM, using LambdaOutputBuffer") // PROFILE
              outputBuffer = Some(new LambdaOutputBuffer[T, U](f, acc.get))
            }
          }
        }

        outputBuffer.get.next
      }

      def hasNext : Boolean = {
        val nonEmpty = (nested.hasNext || (!outputBuffer.isEmpty && outputBuffer.get.hasNext))
        if (!nonEmpty && !ctxCache.isEmpty) {
          val iter : java.util.Iterator[java.util.Map.Entry[Long, Long]] = ctxCache.entrySet.iterator
          while (iter.hasNext) {
              val curr : java.util.Map.Entry[Long, Long] = iter.next
              OpenCLBridge.cleanupSwatContext(curr.getValue)
          }
          ctxCache.clear
          System.err.println("SWAT PROF " + threadId + " Processed " + totalNLoaded + // PROFILE
              " elements") // PROFILE
          profPrint("Total", overallStart, threadId) // PROFILE
        }
        nonEmpty
      }
    }

    iter
  }

  // override def map[V: ClassTag](f: U => V): RDD[V] = {
  //   new CLMappedRDD(this, sparkContext.clean(f))
  // }
}
