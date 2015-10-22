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

  override def toString() : String = {
    "[" + kernel + "," + dev_ctx + "]"
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
  val nNativeInputBuffers_str = System.getProperty("swat.n_native_input_buffers")
  val nNativeInputBuffers : Int = if (nNativeInputBuffers_str != null) nNativeInputBuffers_str.toInt else 2

  val nNativeOutputBuffers_str = System.getProperty("swat.n_native_output_buffers")
  val nNativeOutputBuffers : Int = if (nNativeOutputBuffers_str != null) nNativeOutputBuffers_str.toInt else 2

  val N_str = System.getProperty("swat.input_chunking")
  val N = if (N_str != null) N_str.toInt else 50000

  val heapSize_str = System.getProperty("swat.heap_size")
  val heapSize = if (heapSize_str != null) heapSize_str.toInt else 64 * 1024 * 1024

  val percHighPerfBuffers_str = System.getProperty("swat.perc_high_perf_buffers")
  val percHighPerfBuffers = if (percHighPerfBuffers_str != null)
        percHighPerfBuffers_str.toDouble else 0.2

  val cl_local_size_str = System.getProperty("swat.cl_local_size")
  val cl_local_size = if (cl_local_size_str != null) cl_local_size_str.toInt else 128

  val spark_cores_str = System.getenv("SPARK_WORKER_CORES")
  assert(spark_cores_str != null)
  val spark_cores = spark_cores_str.toInt

  val heapsPerDevice_str = System.getProperty("swat.heaps_per_device")
  val heapsPerDevice = if (heapsPerDevice_str != null) heapsPerDevice_str.toInt
        else CLMappedRDDStorage.spark_cores

  val kernelDir_str = System.getProperty("swat.kernels_dir")
  val kernelDir = if (kernelDir_str != null) kernelDir_str else "/tmp/"

  val printKernel_str = System.getProperty("swat.print_kernel")
  val printKernel = if (printKernel_str != null) printKernel_str.toBoolean else false

  val swatContextCache : java.util.Map[Integer, java.util.HashMap[KernelDevicePair, Long]] =
        new java.util.HashMap[Integer, java.util.HashMap[KernelDevicePair, Long]]()
  val inputBufferCache : java.util.Map[Integer, java.util.HashMap[String, InputBufferWrapper[_]]] =
        new java.util.HashMap[Integer, java.util.HashMap[String, InputBufferWrapper[_]]]()
  val outputBufferCache : java.util.Map[Integer, java.util.HashMap[String, OutputBufferWrapper[_]]] =
        new java.util.HashMap[Integer, java.util.HashMap[String, OutputBufferWrapper[_]]]()

  def getCacheFor[K, V](threadId : Int,
      globalCache : java.util.Map[Integer, java.util.HashMap[K, V]]) :
        java.util.HashMap[K, V] = {
    globalCache.synchronized {
      if (globalCache.containsKey(threadId)) {
        globalCache.get(threadId)
      } else {
        val newCache : java.util.HashMap[K, V] = new java.util.HashMap[K, V]()
        globalCache.put(threadId, newCache)
        newCache
      }
    }
  }
}

/*
 * A new CLMappedRDD object is created for each partition/task being processed,
 * lifetime and accessibility of items inside an instance of these is limited to
 * one thread and one task running on that thread.
 */
class CLMappedRDD[U: ClassTag, T: ClassTag](val prev: RDD[T], val f: T => U) extends RDD[U](prev) {
  var entryPoint : Entrypoint = null
  var openCL : String = null

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) : Iterator[U] = {
    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    System.setProperty("com.amd.aparapi.enable.ATHROW", "true");

    var inputBuffer : InputBufferWrapper[T] = null
    var chunkedOutputBuffer : OutputBufferWrapper[U] = null
    var outputBuffer : Option[OutputBufferWrapper[U]] = None

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

    val threadId : Int = RuntimeUtil.getThreadID

    /*
     * A queue of native input buffers that are ready to be read into by the
     * reader thread.
     */
    val initiallyEmptyNativeInputBuffers : java.util.LinkedList[NativeInputBuffers[T]] =
            new java.util.LinkedList[NativeInputBuffers[T]]()
    assert(CLMappedRDDStorage.nNativeInputBuffers >= 2)
    val nativeInputBuffersArray : Array[NativeInputBuffers[T]] =
            new Array[NativeInputBuffers[T]](CLMappedRDDStorage.nNativeInputBuffers)
    val nativeOutputBuffersArray : Array[NativeOutputBuffers[U]] =
            new Array[NativeOutputBuffers[U]](CLMappedRDDStorage.nNativeOutputBuffers)
    /*
     * Native input buffers that have been filled by the reader thread and are
     * ready to be copied to the device.
     */
    val filledNativeInputBuffers : java.util.LinkedList[NativeInputBuffers[T]] =
            new java.util.LinkedList[NativeInputBuffers[T]]()
    /*
     * Native output buffers that have been emptied out by the writer thread and
     * can be used by a new kernel to store outputs.
     */
    val emptiedNativeOutputBuffers : java.util.LinkedList[NativeOutputBuffers[U]] =
            new java.util.LinkedList[NativeOutputBuffers[U]]()

    System.err.println("Thread=" + threadId + " N = " + CLMappedRDDStorage.N +
            ", cl_local_size = " + CLMappedRDDStorage.cl_local_size +
            ", spark_cores = " + CLMappedRDDStorage.spark_cores + ", stage = " +
            context.stageId + ", partition = " + context.partitionId)
    val nested = firstParent[T].iterator(split, context)

    val myInputBufferCache : java.util.HashMap[String, InputBufferWrapper[_]] =
      CLMappedRDDStorage.getCacheFor(threadId, CLMappedRDDStorage.inputBufferCache)
    val myOutputBufferCache : java.util.HashMap[String, OutputBufferWrapper[_]] =
      CLMappedRDDStorage.getCacheFor(threadId, CLMappedRDDStorage.outputBufferCache)

    if (myInputBufferCache.containsKey(f.getClass.getName)) {
      assert(inputBuffer == null)
      assert(chunkedOutputBuffer == null)

      inputBuffer = myInputBufferCache.get(f.getClass.getName)
          .asInstanceOf[InputBufferWrapper[T]]
      chunkedOutputBuffer = myOutputBufferCache.get(f.getClass.getName)
          .asInstanceOf[OutputBufferWrapper[U]]
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

//    val deviceInitStart = System.currentTimeMillis // PROFILE
    val device_index = OpenCLBridge.getDeviceToUse(partitionDeviceHint,
            threadId, CLMappedRDDStorage.heapsPerDevice,
            CLMappedRDDStorage.heapSize, CLMappedRDDStorage.percHighPerfBuffers,
            false)
//    System.err.println("Thread " + threadId + " selected device " + device_index) // PROFILE
    val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index,
            CLMappedRDDStorage.heapsPerDevice, CLMappedRDDStorage.heapSize,
            CLMappedRDDStorage.percHighPerfBuffers, false)
    val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)
//    RuntimeUtil.profPrint("DeviceInit", deviceInitStart, threadId) // PROFILE

   val firstSample : T = nested.next
   var firstBufferOp : Boolean = true
   var sampleOutput : java.lang.Object = None

   val initializing : Boolean = (entryPoint == null)

   if (initializing) {
//      val initStart = System.currentTimeMillis // PROFILE
     sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
     val entrypointAndKernel : Tuple2[Entrypoint, String] =
         RuntimeUtil.getEntrypointAndKernel[T, U](firstSample, sampleOutput,
         params, f, classModel, descriptor, dev_ctx, threadId,
         CLMappedRDDStorage.kernelDir, CLMappedRDDStorage.printKernel)
     entryPoint = entrypointAndKernel._1
     openCL = entrypointAndKernel._2

     if (inputBuffer == null) {
       inputBuffer = RuntimeUtil.getInputBufferForSample(firstSample, CLMappedRDDStorage.N,
               DenseVectorInputBufferWrapperConfig.tiling,
               SparseVectorInputBufferWrapperConfig.tiling,
               entryPoint, false)
       myInputBufferCache.put(f.getClass.getName, inputBuffer)

       chunkedOutputBuffer = OpenCLBridgeWrapper.getOutputBufferFor[U](
               sampleOutput.asInstanceOf[U], CLMappedRDDStorage.N,
               entryPoint, devicePointerSize, CLMappedRDDStorage.heapSize)
       myOutputBufferCache.put(f.getClass.getName,
               chunkedOutputBuffer.asInstanceOf[OutputBufferWrapper[_]])
     }

//      RuntimeUtil.profPrint("Initialization", initStart, threadId) // PROFILE
   }

   val outArgNum : Int = inputBuffer.countArgumentsUsed
   /*
    * When used as an output, only Tuple2 uses two arguments. All other types
    * use one.
    */
   val nOutArgs : Int = if (sampleOutput.isInstanceOf[Tuple2[_, _]]) 2 else 1
   var heapArgStart : Int = -1
   var lastArgIndex : Int = -1
   var heapTopArgNum : Int = -1

   for (i <- 0 until CLMappedRDDStorage.nNativeInputBuffers) {
     val newBuffer : NativeInputBuffers[T] = inputBuffer.generateNativeInputBuffer(dev_ctx)
     newBuffer.id = i
     initiallyEmptyNativeInputBuffers.add(newBuffer)
     nativeInputBuffersArray(i) = newBuffer
   }

   for (i <- 0 until CLMappedRDDStorage.nNativeOutputBuffers) {
     val newBuffer : NativeOutputBuffers[U] =
         chunkedOutputBuffer.generateNativeOutputBuffer(dev_ctx, outArgNum)
     newBuffer.id = i
     emptiedNativeOutputBuffers.add(newBuffer)
     nativeOutputBuffersArray(i) = newBuffer
   }

//    val ctxCreateStart = System.currentTimeMillis // PROFILE
   val mySwatContextCache : java.util.HashMap[KernelDevicePair, Long] =
       CLMappedRDDStorage.getCacheFor(threadId,
               CLMappedRDDStorage.swatContextCache)

   val kernelDeviceKey : KernelDevicePair = new KernelDevicePair(
           f.getClass.getName, dev_ctx)
   if (!mySwatContextCache.containsKey(kernelDeviceKey)) {
     val ctx : Long = OpenCLBridge.createSwatContext(
               f.getClass.getName, openCL, dev_ctx, threadId,
               entryPoint.requiresDoublePragma,
               entryPoint.requiresHeap, CLMappedRDDStorage.N)
     mySwatContextCache.put(kernelDeviceKey, ctx)
   }
   val ctx : Long = mySwatContextCache.get(kernelDeviceKey)
   OpenCLBridge.resetSwatContext(ctx)

   try {
     var argnum = outArgNum + nOutArgs
     
     val iter = entryPoint.getReferencedClassModelFields.iterator
     while (iter.hasNext) {
       val field = iter.next
       val isBroadcast = entryPoint.isBroadcastField(field)
       argnum += OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, argnum, f,
           field.getName, field.getDescriptor, entryPoint, isBroadcast)
     }

     if (entryPoint.requiresHeap) {
       heapArgStart = argnum
       heapTopArgNum = heapArgStart + 1
       lastArgIndex = heapArgStart + 4
     } else {
       lastArgIndex = argnum
     }
   } catch {
     case oomExc : OpenCLOutOfMemoryException => {
//        System.err.println("SWAT PROF " + threadId + // PROFILE
//                " OOM during initialization, using LambdaOutputBuffer") // PROFILE
       OpenCLBridge.cleanupArguments(ctx)

       return new Iterator[U] {
         val nested = firstParent[T].iterator(split, context)
         override def next() : U = { f(nested.next) }
         override def hasNext() : Boolean = { nested.hasNext }
       }
     }
   }
   /*
    * Flush out the kernel arguments that never change, broadcast variables and
    * closure-captured variables
    */
   OpenCLBridge.setupGlobalArguments(ctx, dev_ctx) 

    val iter = new Iterator[U] {

     /* BEGIN READER THREAD */
     var noMoreInputBuffering = false
     var lastSeqNo : Int = -1
     val readerRunner = new Runnable() {
       override def run() {
         var done : Boolean = false
         inputBuffer.setCurrentNativeBuffers(
                 initiallyEmptyNativeInputBuffers.remove)

         while (!done) {
//            val ioStart = System.currentTimeMillis // PROFILE
           inputBuffer.reset

           val myOffset : Int = totalNLoaded
           // val inputCacheId = if (firstParent[T].getStorageLevel.useMemory)
           //     new CLCacheID(firstParent[T].id, split.index, myOffset, 0)
           //     else NoCache
           val inputCacheId = NoCache

           var nLoaded : Int = -1
           val inputCacheSuccess = if (inputCacheId == NoCache) -1 else
             inputBuffer.tryCache(inputCacheId, ctx, dev_ctx, entryPoint, false)

           if (inputCacheSuccess != -1) {
             nLoaded = OpenCLBridge.fetchNLoaded(inputCacheId.rdd, inputCacheId.partition,
               inputCacheId.offset)
             /*
              * Drop the rest of this buffer from the input stream if cached,
              * accounting for the fact that some items may already have been
              * buffered from it, either in the inputBuffer (due to an overrun) or
              * because of firstSample.
              */
             val nAlreadyBuffered = if (firstBufferOp)
                    nLoaded - 1 - inputBuffer.nBuffered else
                    nLoaded - inputBuffer.nBuffered
             nested.drop(nAlreadyBuffered)
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

//            RuntimeUtil.profPrint("Input-I/O", ioStart, threadId) // PROFILE
           System.err.println("SWAT PROF " + threadId + " Loaded " + nLoaded) // PROFILE

           /*
            * Now that we're done loading from the input stream, fetch the next
            * native input buffers to transfer any overflow into.
            */
           val nextNativeInputBuffer : NativeInputBuffers[T] =
               if (!initiallyEmptyNativeInputBuffers.isEmpty) {
                 initiallyEmptyNativeInputBuffers.remove
               } else {
                 val id : Int = OpenCLBridge.waitForFreedNativeBuffer(ctx, dev_ctx)
                 nativeInputBuffersArray(id)
               }

           /*
            * Transfer overflow from inputBuffer.nativeBuffers/filled to
            * nextNativeInputBuffer.
            */
           inputBuffer.setupNativeBuffersForCopy(-1)
           val filled : NativeInputBuffers[T] = inputBuffer.transferOverflowTo(
                   nextNativeInputBuffer)
           assert(filled.id != nextNativeInputBuffer.id)

           // Transfer input to device asynchronously
           if (filled.clBuffersReadyPtr != 0L) {
               OpenCLBridge.waitOnBufferReady(filled.clBuffersReadyPtr)
           }
           filled.copyToDevice(0, ctx, dev_ctx, inputCacheId, false)
           /*
            * Add a callback to notify the reader thread that a native input
            * buffer is now available
            */
           OpenCLBridge.enqueueBufferFreeCallback(ctx, dev_ctx, filled.id)

           val currentNativeOutputBuffer : NativeOutputBuffers[U] =
                emptiedNativeOutputBuffers.synchronized {
                  while (emptiedNativeOutputBuffers.isEmpty) {
                    emptiedNativeOutputBuffers.wait
                  }
                  emptiedNativeOutputBuffers.poll
                }
           currentNativeOutputBuffer.addToArgs

           if (entryPoint.requiresHeap) {
             // processing_succeeded
             if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, heapTopArgNum + 2,
                     CLMappedRDDStorage.N * 4, false)) {
               throw new OpenCLOutOfMemoryException();
             }
           }

           OpenCLBridge.setIntArg(ctx, lastArgIndex, nLoaded)

           done = !nested.hasNext && !inputBuffer.haveUnprocessedInputs
           val currSeqNo : Int = OpenCLBridge.getCurrentSeqNo(ctx)
           if (done) {
             /*
              * This should come before OpenCLBridge.run to ensure there is no
              * race between setting lastSeqNo and the writer thread checking it
              * after kernel completion.
              */
             assert(lastSeqNo == -1)
             lastSeqNo = currSeqNo
             inputBuffer.setCurrentNativeBuffers(null)
           }

           val doneFlag : Long = OpenCLBridge.run(ctx, dev_ctx, nLoaded,
                   CLMappedRDDStorage.cl_local_size, lastArgIndex + 1,
                   heapArgStart, CLMappedRDDStorage.heapsPerDevice,
                   currentNativeOutputBuffer.id)
           filled.clBuffersReadyPtr = doneFlag
         }
       }
     }
     var readerThread : Thread = new Thread(readerRunner)
     readerThread.start
     /* END READER THREAD */

//      RuntimeUtil.profPrint("ContextCreation", ctxCreateStart, threadId) // PROFILE

     var curr_kernel_ctx : Long = 0L
     var currentNativeOutputBuffer : NativeOutputBuffers[U] = null
     var curr_seq_no : Int = 0
     def next() : U = {
       if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
         if (curr_kernel_ctx > 0L) {
           assert(currentNativeOutputBuffer != null)
           OpenCLBridge.cleanupKernelContext(curr_kernel_ctx)
           emptiedNativeOutputBuffers.synchronized {
             emptiedNativeOutputBuffers.add(currentNativeOutputBuffer)
             emptiedNativeOutputBuffers.notify
           }
         }
         curr_kernel_ctx = OpenCLBridge.waitForFinishedKernel(ctx, dev_ctx,
                 curr_seq_no)
         curr_seq_no += 1

         val current_output_buffer_id =
                 OpenCLBridge.getOutputBufferIdFromKernelCtx(curr_kernel_ctx)
         currentNativeOutputBuffer = nativeOutputBuffersArray(
                 current_output_buffer_id)

         //TODO and create various native output buffer classes
         chunkedOutputBuffer.fillFrom(curr_kernel_ctx, outArgNum)

         outputBuffer = Some(chunkedOutputBuffer)
       }

       outputBuffer.get.next
     }

     def hasNext() : Boolean = {
       /*
        * hasNext may be called multiple times after running out of items, in
        * which case inputBuffer may already be null on entry to this function.
        */
       val haveNext = inputBuffer != null && (lastSeqNo == -1 ||
               curr_seq_no <= lastSeqNo || outputBuffer.get.hasNext)
       if (!haveNext && inputBuffer != null) {
         inputBuffer = null
         chunkedOutputBuffer = null

         for (buffer <- nativeInputBuffersArray) {
           buffer.releaseOpenCLArrays
         }
         for (buffer <- nativeOutputBuffersArray) {
           buffer.releaseOpenCLArrays
         }

         OpenCLBridge.cleanupKernelContext(curr_kernel_ctx)
         OpenCLBridge.cleanupSwatContext(ctx, dev_ctx)
//          RuntimeUtil.profPrint("Total", overallStart, threadId) // PROFILE
//          System.err.println("SWAT PROF Total loaded = " + totalNLoaded) // PROFILE
       }
       haveNext
     }
    }

    iter
  }
}
