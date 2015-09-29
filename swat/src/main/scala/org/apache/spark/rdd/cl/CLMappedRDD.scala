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

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    // val N = 65536 * 8
    val N = 100000
    var inputBuffer : InputBufferWrapper[T] = null
    var nativeOutputBuffer : Option[OutputBufferWrapper[U]] = None
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

      var totalNLoaded = 0
      val overallStart = System.currentTimeMillis

     val partitionDeviceHint : Int = OpenCLBridge.getDeviceHintFor(
             firstParent[T].id, split.index, totalNLoaded, 0)

//      val deviceInitStart = System.currentTimeMillis // PROFILE
     val device_index = OpenCLBridge.getDeviceToUse(partitionDeviceHint, threadId)
//      System.err.println("Selected device " + device_index) // PROFILE
     val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index)
     val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)
//      RuntimeUtil.profPrint("DeviceInit", deviceInitStart, threadId) // PROFILE

     val firstSample : T = nested.next
     var firstBufferOp : Boolean = true
     var sampleOutput : java.lang.Object = None

     val initializing : Boolean = (entryPoint == null)

     if (initializing) {
//        val initStart = System.currentTimeMillis // PROFILE
       sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
       val entrypointAndKernel : Tuple2[Entrypoint, String] =
           RuntimeUtil.getEntrypointAndKernel[T, U](firstSample, sampleOutput,
           params, f, classModel, descriptor, dev_ctx, threadId)
       entryPoint = entrypointAndKernel._1
       openCL = entrypointAndKernel._2

       inputBuffer = RuntimeUtil.getInputBufferFor(firstSample, N, entryPoint)

       nativeOutputBuffer = Some(OpenCLBridgeWrapper.getOutputBufferFor[U](
                   sampleOutput.asInstanceOf[U], N, entryPoint))
//        RuntimeUtil.profPrint("Initialization", initStart, threadId) // PROFILE
     }

//      val ctxCreateStart = System.currentTimeMillis // PROFILE
     if (!ctxCache.containsKey(dev_ctx)) {
       ctxCache.put(dev_ctx, OpenCLBridge.createSwatContext(
                   f.getClass.getName, openCL, dev_ctx, threadId,
                   entryPoint.requiresDoublePragma,
                   entryPoint.requiresHeap, N))
     }
     val ctx : Long = ctxCache.get(dev_ctx)
//      RuntimeUtil.profPrint("ContextCreation", ctxCreateStart, threadId) // PROFILE

     def next() : U = {
       if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
         inputBuffer.reset
         nativeOutputBuffer.get.reset

         val myOffset : Int = totalNLoaded
         val inputCacheId = if (firstParent[T].getStorageLevel.useMemory)
             new CLCacheID(firstParent[T].id, split.index, myOffset, 0)
             else NoCache

         var nLoaded : Int = -1
         val inputCacheSuccess : Int = if (inputCacheId == NoCache) -1 else
           inputBuffer.tryCache(inputCacheId, ctx, dev_ctx, entryPoint)

         if (inputCacheSuccess != -1) {
           nLoaded = OpenCLBridge.fetchNLoaded(inputCacheId.rdd, inputCacheId.partition,
             inputCacheId.offset)
           if (firstBufferOp) {
             nested.drop(nLoaded - 1)
           } else {
             nested.drop(nLoaded)
           }
         } else {
           if (firstBufferOp) {
             inputBuffer.append(firstSample)
           }
           inputBuffer.aggregateFrom(nested)
           inputBuffer.flush

           nLoaded = inputBuffer.nBuffered
           if (inputCacheId != NoCache) {
             OpenCLBridge.storeNLoaded(inputCacheId.rdd, inputCacheId.partition,
               inputCacheId.offset, nLoaded)
           }
         }
         firstBufferOp = false
         totalNLoaded += nLoaded

//          RuntimeUtil.profPrint("Input-I/O", ioStart, threadId) // PROFILE
//          System.err.println("SWAT PROF " + threadId + " Loaded " + nLoaded) // PROFILE

         try {

           // Only try to cache on GPU if the programmer has cached it in memory
           var argnum : Int = if (inputCacheSuccess == -1)
             inputBuffer.copyToDevice(0, ctx, dev_ctx, inputCacheId) else
             inputCacheSuccess

//            val writeStart = System.currentTimeMillis // PROFILE

           val outArgNum : Int = argnum
           argnum += OpenCLBridgeWrapper.setUnitializedArrayArg[U](ctx,
               dev_ctx, argnum, N, classTag[U].runtimeClass,
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
                     N)
           }
//            RuntimeUtil.profPrint("Write", writeStart, threadId) // PROFILE

           OpenCLBridge.setIntArg(ctx, argnum, nLoaded)
           val anyFailedArgNum = argnum - 1

//            val runStart = System.currentTimeMillis // PROFILE
           var complete : Boolean = true
//            var ntries : Int = 0 // PROFILE
           do {
             OpenCLBridge.run(ctx, dev_ctx, nLoaded)
             if (entryPoint.requiresHeap) {
               complete = nativeOutputBuffer.get.kernelAttemptCallback(
                       nLoaded, anyFailedArgNum, heapArgStart + 3, outArgNum,
                       heapArgStart, heapSize, ctx, dev_ctx,
                       devicePointerSize)
               if (!complete) {
                 OpenCLBridge.resetHeap(ctx, dev_ctx, heapArgStart)
               }
             }
//              ntries += 1 // PROFILE
           } while (!complete)

//            RuntimeUtil.profPrint("Run", runStart, threadId) // PROFILE
//            System.err.println("Thread " + threadId + " performed " + ntries + " kernel retries") // PROFILE
//            val readStart = System.currentTimeMillis // PROFILE

           nativeOutputBuffer.get.finish(ctx, dev_ctx, outArgNum, nLoaded)
           OpenCLBridge.postKernelCleanup(ctx);
           outputBuffer = Some(nativeOutputBuffer.get)

//            RuntimeUtil.profPrint("Read", readStart, threadId) // PROFILE
         } catch {
           case oom : OpenCLOutOfMemoryException => {
//              System.err.println("SWAT PROF " + threadId + " OOM, using LambdaOutputBuffer") // PROFILE
             outputBuffer = Some(new LambdaOutputBuffer[T, U](f, inputBuffer))
           }
         }

       }

       outputBuffer.get.next

       // f(inputBuffer.next)
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
       val haveNext = nested.hasNext || outputBuffer.get.hasNext
       if (!haveNext) {
         inputBuffer.releaseNativeArrays
         nativeOutputBuffer.get.releaseNativeArrays
         inputBuffer = null
         nativeOutputBuffer = null
       }
       haveNext
     }
    }

    iter
  }
}
