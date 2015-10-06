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

class KernelDevicePair(val kernel : String, val dev_ctx : Long) {

  override def equals(obj : Any) : Boolean = {
    if (obj.isInstanceOf[KernelDevicePair]) {
      val other : KernelDevicePair = obj.asInstanceOf[KernelDevicePair]
      kernel.equals(other.kernel) && dev_ctx == other.dev_ctx
    } else {
      false
    }
  }

  override def hashCode() : Int = {
    dev_ctx.toInt
  }
}

/*
 * Shared by all threads within a single node across a whole job, multiple tasks
 * of multiple types from multiple threads may share the data stored here.
 *
 * Initialized single-threaded.
 *
 * I believe this might actually be run single-threaded in the driver, then
 * captured in the closure handed to the workers? Having data in here might
 * drastically increase the size of the data transmitted.
 */
object CLMappedRDDStorage {
  val N_str = System.getProperty("swat.input_chunking")
  val N = if (N_str != null) N_str.toInt else 50000

  val cl_local_size_str = System.getProperty("swat.cl_local_size")
  val cl_local_size = if (cl_local_size_str != null) cl_local_size_str.toInt else 128

  val spark_cores_str = System.getenv("SPARK_WORKER_CORES")
  assert(spark_cores_str != null)
  val spark_cores = spark_cores_str.toInt

  val swatContextCache : Array[java.util.HashMap[KernelDevicePair, Long]] =
        new Array[java.util.HashMap[KernelDevicePair, Long]](spark_cores)
  val inputBufferCache : Array[java.util.HashMap[String, InputBufferWrapper[_]]] =
      new Array[java.util.HashMap[String, InputBufferWrapper[_]]](spark_cores)
  val outputBufferCache : Array[java.util.HashMap[String, OutputBufferWrapper[_]]] =
      new Array[java.util.HashMap[String, OutputBufferWrapper[_]]](spark_cores)
  for (i <- 0 until spark_cores) {
    swatContextCache(i) = new java.util.HashMap[KernelDevicePair, Long]()
    inputBufferCache(i) = new java.util.HashMap[String, InputBufferWrapper[_]]()
    outputBufferCache(i) = new java.util.HashMap[String, OutputBufferWrapper[_]]()
  }
}

/*
 * A new CLMappedRDD object is created for each partition/task being processed,
 * lifetime and accessibility of items inside an instance of these is limited to
 * one thread and one task running on that thread.
 */
class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
    extends RDD[U](prev) {
  val heapSize = 100 * 1024 * 1024
  var entryPoint : Entrypoint = null
  var openCL : String = null

  var inputBuffer : InputBufferWrapper[T] = null
  var nativeOutputBuffer : OutputBufferWrapper[U] = null
  var outputBuffer : Option[OutputBufferWrapper[U]] = None

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    System.setProperty("com.amd.aparapi.enable.ATHROW", "true");

/*
    if (threadId % 3 == 0) {
      // Every 2 threads runs on the JVM
      return new Iterator[U] {
        val nested = firstParent[T].iterator(split, context)

        def next() : U = {
          f(nested.next)
        }

        def hasNext() : Boolean = {
          nested.hasNext
        }
      }
    }
*/

    val iter = new Iterator[U] {
      val threadId : Int = RuntimeUtil.getThreadID()

      System.err.println("Thread=" + threadId + " N=" + CLMappedRDDStorage.N +
              ", cl_local_size=" + CLMappedRDDStorage.cl_local_size +
              ", spark_cores=" + CLMappedRDDStorage.spark_cores)
      val nested = firstParent[T].iterator(split, context)

      if (CLMappedRDDStorage.inputBufferCache(threadId).containsKey(f.getClass.getName)) {
        inputBuffer = CLMappedRDDStorage.inputBufferCache(threadId).get(
                f.getClass.getName).asInstanceOf[InputBufferWrapper[T]]
        nativeOutputBuffer = CLMappedRDDStorage.outputBufferCache(threadId).get(
                f.getClass.getName).asInstanceOf[OutputBufferWrapper[U]]
      } else {
        inputBuffer = null
        nativeOutputBuffer = null
      }

      val classModel : ClassModel = ClassModel.createClassModel(f.getClass, null,
          new ShouldNotCallMatcher())
      val method = classModel.getPrimitiveApplyMethod
      val descriptor : String = method.getDescriptor

      // 1 argument expected for maps
      val params : LinkedList[ScalaArrayParameter] =
          CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, 1)
      params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

      var totalNLoaded = 0
      val overallStart = System.currentTimeMillis

      val partitionDeviceHint : Int = OpenCLBridge.getDeviceHintFor(
              firstParent[T].id, split.index, totalNLoaded, 0)

     val deviceInitStart = System.currentTimeMillis // PROFILE
      val device_index = OpenCLBridge.getDeviceToUse(partitionDeviceHint, threadId)
     System.err.println("Selected device " + device_index) // PROFILE
      val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index)
      val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)
     RuntimeUtil.profPrint("DeviceInit", deviceInitStart, threadId) // PROFILE

     val firstSample : T = nested.next
     var firstBufferOp : Boolean = true
     var sampleOutput : java.lang.Object = None

     val initializing : Boolean = (entryPoint == null)

     if (initializing) {

       val initStart = System.currentTimeMillis // PROFILE
       sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
       val entrypointAndKernel : Tuple2[Entrypoint, String] =
           RuntimeUtil.getEntrypointAndKernel[T, U](firstSample, sampleOutput,
           params, f, classModel, descriptor, dev_ctx, threadId)
       entryPoint = entrypointAndKernel._1
       openCL = entrypointAndKernel._2

       if (inputBuffer == null) {
         inputBuffer = RuntimeUtil.getInputBufferFor(firstSample, CLMappedRDDStorage.N,
                 DenseVectorInputBufferWrapperConfig.tiling, entryPoint)
         CLMappedRDDStorage.inputBufferCache(threadId).put(f.getClass.getName, inputBuffer)
         nativeOutputBuffer = OpenCLBridgeWrapper.getOutputBufferFor[U](
                     sampleOutput.asInstanceOf[U], CLMappedRDDStorage.N, entryPoint)
         CLMappedRDDStorage.outputBufferCache(threadId).put(f.getClass.getName, nativeOutputBuffer)
       }

       RuntimeUtil.profPrint("Initialization", initStart, threadId) // PROFILE
     }

     val ctxCreateStart = System.currentTimeMillis // PROFILE
     val kernelDeviceKey : KernelDevicePair = new KernelDevicePair(
             f.getClass.getName, dev_ctx)
     if (!CLMappedRDDStorage.swatContextCache(threadId).containsKey(kernelDeviceKey)) {
       val ctx : Long = OpenCLBridge.createSwatContext(
                 f.getClass.getName, openCL, dev_ctx, threadId,
                 entryPoint.requiresDoublePragma,
                 entryPoint.requiresHeap, CLMappedRDDStorage.N)
       CLMappedRDDStorage.swatContextCache(threadId).put(kernelDeviceKey, ctx)
     }
     val ctx : Long = CLMappedRDDStorage.swatContextCache(threadId).get(
             kernelDeviceKey)
     RuntimeUtil.profPrint("ContextCreation", ctxCreateStart, threadId) // PROFILE

     def next() : U = {
       if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
         inputBuffer.reset
         nativeOutputBuffer.reset

         val ioStart = System.currentTimeMillis // PROFILE

         val myOffset : Int = totalNLoaded
         val inputCacheId = if (firstParent[T].getStorageLevel.useMemory)
             new CLCacheID(firstParent[T].id, split.index, myOffset, 0)
             else NoCache

         var nLoaded : Int = -1
         val inputCacheSuccess : Int = if (inputCacheId == NoCache) -1 else
           inputBuffer.tryCache(inputCacheId, ctx, dev_ctx, entryPoint)

         if (inputCacheSuccess != -1) {
           val handlingCachedStart = System.currentTimeMillis
           nLoaded = OpenCLBridge.fetchNLoaded(inputCacheId.rdd, inputCacheId.partition,
             inputCacheId.offset)
           /*
            * Drop the rest of this buffer from the input stream if cached,
            * accounting for the fact that some items may already have been
            * buffered from it, either in the inputBuffer (due to an overrun) or
            * because of firstSample.
            */
           if (firstBufferOp) {
             nested.drop(nLoaded - 1 - inputBuffer.nBuffered)
           } else {
             nested.drop(nLoaded - inputBuffer.nBuffered)
           }
           RuntimeUtil.profPrint("Cached", handlingCachedStart, threadId) // PROFILE
         } else {
           if (firstBufferOp) {
             inputBuffer.append(firstSample)
           }

           val handlingUncachedStart = System.currentTimeMillis // PROFILE
           inputBuffer.aggregateFrom(nested)
           RuntimeUtil.profPrint("Aggregating", handlingUncachedStart, threadId) // PROFILE

           inputBuffer.flush

           nLoaded = inputBuffer.nBuffered
           if (inputCacheId != NoCache) {
             OpenCLBridge.storeNLoaded(inputCacheId.rdd, inputCacheId.partition,
               inputCacheId.offset, nLoaded)
           }
           RuntimeUtil.profPrint("Uncached", handlingUncachedStart, threadId) // PROFILE
         }
         firstBufferOp = false
         totalNLoaded += nLoaded

         RuntimeUtil.profPrint("Input-I/O", ioStart, threadId) // PROFILE
         System.err.println("SWAT PROF " + threadId + " Loaded " + nLoaded) // PROFILE

         try {

           // Only try to cache on GPU if the programmer has cached it in memory
           var argnum : Int = if (inputCacheSuccess == -1)
             inputBuffer.copyToDevice(0, ctx, dev_ctx, inputCacheId) else
             inputCacheSuccess

           val writeStart = System.currentTimeMillis // PROFILE

           val outArgNum : Int = argnum
           argnum += OpenCLBridgeWrapper.setUnitializedArrayArg[U](ctx,
               dev_ctx, argnum, CLMappedRDDStorage.N, classTag[U].runtimeClass,
               entryPoint, sampleOutput.asInstanceOf[U])

           val iter = entryPoint.getReferencedClassModelFields.iterator
           while (iter.hasNext) {
             val field = iter.next
             val isBroadcast = entryPoint.isBroadcastField(field)
             argnum += OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, argnum, f,
                 field.getName, field.getDescriptor, entryPoint, isBroadcast)
           }

           val heapArgStart : Int = argnum
           if (entryPoint.requiresHeap) {
             argnum += OpenCLBridge.createHeap(ctx, dev_ctx, argnum, heapSize,
                     CLMappedRDDStorage.N)
           }
           RuntimeUtil.profPrint("Write", writeStart, threadId) // PROFILE

           OpenCLBridge.setIntArg(ctx, argnum, nLoaded)
           val anyFailedArgNum = argnum - 1

           val runStart = System.currentTimeMillis // PROFILE
           var complete : Boolean = true
           var ntries : Int = 0 // PROFILE
           do {
             OpenCLBridge.run(ctx, dev_ctx, nLoaded, CLMappedRDDStorage.cl_local_size)
             if (entryPoint.requiresHeap) {
               complete = nativeOutputBuffer.kernelAttemptCallback(
                       nLoaded, anyFailedArgNum, heapArgStart + 3, outArgNum,
                       heapArgStart, heapSize, ctx, dev_ctx,
                       devicePointerSize)
               if (!complete) {
                 OpenCLBridge.resetHeap(ctx, dev_ctx, heapArgStart)
               }
             }
             ntries += 1 // PROFILE
           } while (!complete)

           RuntimeUtil.profPrint("Run", runStart, threadId) // PROFILE
           System.err.println("Thread " + threadId + " performed " + ntries + " kernel retries") // PROFILE
           val readStart = System.currentTimeMillis // PROFILE

           nativeOutputBuffer.finish(ctx, dev_ctx, outArgNum, nLoaded)
           OpenCLBridge.postKernelCleanup(ctx);
           outputBuffer = Some(nativeOutputBuffer)

           RuntimeUtil.profPrint("Read", readStart, threadId) // PROFILE
         } catch {
           case oom : OpenCLOutOfMemoryException => {
             System.err.println("SWAT PROF " + threadId + " OOM, using LambdaOutputBuffer") // PROFILE
             outputBuffer = Some(new LambdaOutputBuffer[T, U](f, inputBuffer))
           }
         }

       }

       outputBuffer.get.next

       /*
       if (firstSample.isEmpty) {
         f(nested.next)
       } else {
         val save : T = firstSample.get
         firstSample = None
         f(save)
       }
       */
     }

     def hasNext() : Boolean = {
       /*
        * hasNext may be called multiple times after running out of items, in
        * which case inputBuffer may already be null on entry to this function.
        */
       val haveNext = inputBuffer != null && (nested.hasNext || outputBuffer.get.hasNext)
       if (!haveNext && inputBuffer != null) {
         inputBuffer = null
         nativeOutputBuffer = null
         RuntimeUtil.profPrint("Total", overallStart, threadId) // PROFILE
       }
       haveNext
     }
    }

    iter
  }
}
