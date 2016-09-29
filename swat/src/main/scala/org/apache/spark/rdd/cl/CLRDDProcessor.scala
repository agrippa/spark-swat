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

/*
 * A pairing of a compiled and executable kernel with a handle for a certain
 * device.
 */
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
 * An abstract, thread-local (and therefore thread-unsafe) caching structure
 * that maps from arbitrary keys to a collection of values for that key.
 */
class PerThreadCache[K, V] {
  val cache : java.util.HashMap[K, java.util.LinkedList[V]] =
      new java.util.HashMap[K, java.util.LinkedList[V]]()

  def hasAny(key : K) : Boolean = {
    cache.containsKey(key) && !cache.get(key).isEmpty
  }

  def get(key : K) : V = {
    cache.get(key).poll
  }

  def add(key : K, value : V) { 
    if (!cache.containsKey(key)) {
      cache.put(key, new java.util.LinkedList[V]())
    }
    cache.get(key).add(value)
  }
}

class GlobalCache[K, V] {
  val cache : java.util.Map[Integer, PerThreadCache[K, V]] =
      new java.util.HashMap[Integer, PerThreadCache[K, V]]()
 
  def forThread(threadId : Int) : PerThreadCache[K, V] = {
    this.synchronized {
      if (cache.containsKey(threadId)) {
        cache.get(threadId)
      } else {
        val newCache = new PerThreadCache[K, V]()
        cache.put(threadId, newCache)
        newCache
      }
    }
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
object CLConfig {
  val nNativeInputBuffers_str = System.getProperty("swat.n_native_input_buffers")
  val nNativeInputBuffers : Int = if (nNativeInputBuffers_str != null)
        nNativeInputBuffers_str.toInt else 2

  val nNativeOutputBuffers_str = System.getProperty("swat.n_native_output_buffers")
  val nNativeOutputBuffers : Int = if (nNativeOutputBuffers_str != null)
        nNativeOutputBuffers_str.toInt else 2

  val N_str = System.getProperty("swat.input_chunking")
  val N = if (N_str != null) N_str.toInt else 50000

  val heapSize_str = System.getProperty("swat.heap_size")
  val heapSize = if (heapSize_str != null) heapSize_str.toInt else
        64 * 1024 * 1024

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
        else CLConfig.spark_cores

  val kernelDir_str = System.getProperty("swat.kernels_dir")
  val kernelDir = if (kernelDir_str != null) kernelDir_str else "/tmp/"

  val printKernel_str = System.getProperty("swat.print_kernel")
  val printKernel = if (printKernel_str != null) printKernel_str.toBoolean else false

  val ASYNC_MAP_LAMBDA =
      "org.apache.spark.rdd.cl.CLAsyncMappedRDD$$anonfun$1"
  val ASYNC_MAP_PARTITIONS_LAMBDA =
      "org.apache.spark.rdd.cl.CLAsyncMapPartitionsRDD$$anonfun$1"

  /*
   * It is possible for a single thread to have two active CLMappedRDDs if they
   * are chained together. For example, the following code:
   *
   *     val rdd1 = CLWrapper.cl(input)
   *     val rdd2 = CLWrapper.cl(rdd1.map(i => i + 1))
   *     val rdd3 = rdd2.map(i => 2 * i)
   *
   * produces two chained RDDs, both doing their maps on the GPU. However, these
   * two instances cannot share input buffers, output buffers, or SWAT contexts
   * despite being in the same thread as they may have concurrently running
   * kernels and input buffering.
   */
  val swatContextCache = new GlobalCache[KernelDevicePair, Long]()
  val inputBufferCache = new GlobalCache[String, InputBufferWrapper[_]]()
  val outputBufferCache = new GlobalCache[String, OutputBufferWrapper[_]]()
}

abstract class CLRDDProcessor[T : ClassTag, U : ClassTag](val nested : Iterator[T],
    val userSample : Option[T], val userLambda : T => U, val context: TaskContext,
    val rddId : Int, val partitionIndex : Int, val pullModel : Boolean)
    extends Iterator[U] {

  if (pullModel) {
    assert(nested != null)
    assert(userSample.isEmpty)
  } else {
    assert(nested == null)
    assert(!userSample.isEmpty)
  }

  var entryPoint : Entrypoint = null
  var openCL : String = null

  System.setProperty("com.amd.aparapi.enable.NEW", "true");
  System.setProperty("com.amd.aparapi.enable.ATHROW", "true");

  var inputBuffer : InputBufferWrapper[T] = null
  var chunkedOutputBuffer : OutputBufferWrapper[U] = null
  var outputBuffer : Option[OutputBufferWrapper[U]] = None

  val threadId : Int = RuntimeUtil.getThreadID

  /*
   * A queue of native input buffers that are ready to be read into by the
   * reader thread.
   */
  val initiallyEmptyNativeInputBuffers : java.util.LinkedList[NativeInputBuffers[T]] =
          new java.util.LinkedList[NativeInputBuffers[T]]()
  assert(CLConfig.nNativeInputBuffers >= 2)
  val nativeInputBuffersArray : Array[NativeInputBuffers[T]] =
          new Array[NativeInputBuffers[T]](CLConfig.nNativeInputBuffers)
  val nativeOutputBuffersArray : Array[NativeOutputBuffers[U]] =
          new Array[NativeOutputBuffers[U]](CLConfig.nNativeOutputBuffers)
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

  System.err.println("Thread = " + threadId + " N = " + CLConfig.N +
          ", cl_local_size = " + CLConfig.cl_local_size +
          ", spark_cores = " + CLConfig.spark_cores + ", stage = " +
          context.stageId + ", partition = " + context.partitionId)

  val myInputBufferCache : PerThreadCache[String, InputBufferWrapper[_]] =
    CLConfig.inputBufferCache.forThread(threadId)
  val myOutputBufferCache : PerThreadCache[String, OutputBufferWrapper[_]] =
    CLConfig.outputBufferCache.forThread(threadId)

  val firstSample : T = if (pullModel) nested.next else userSample.get
  val isAsyncMap = (userLambda.getClass.getName == CLConfig.ASYNC_MAP_LAMBDA ||
      userLambda.getClass.getName == CLConfig.ASYNC_MAP_PARTITIONS_LAMBDA)
  val actualLambda = if (isAsyncMap) firstSample else userLambda
  val bufferKey : String = RuntimeUtil.getLabelForBufferCache(actualLambda, firstSample,
          CLConfig.N)

  if (myInputBufferCache.hasAny(bufferKey)) {
    assert(inputBuffer == null)
    assert(chunkedOutputBuffer == null)

    inputBuffer = myInputBufferCache.get(bufferKey)
        .asInstanceOf[InputBufferWrapper[T]]
    chunkedOutputBuffer = myOutputBufferCache.get(bufferKey)
        .asInstanceOf[OutputBufferWrapper[U]]
  }

  val classModel : ClassModel = ClassModel.createClassModel(actualLambda.getClass, null,
      new ShouldNotCallMatcher())
  val method = classModel.getPrimitiveApplyMethod
  val descriptor : String = method.getDescriptor

  // 1 argument expected for maps
  val params : LinkedList[ScalaArrayParameter] = new LinkedList[ScalaArrayParameter]
  if (!isAsyncMap) {
    params.addAll(CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, 1))
  }
  params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

  var totalNLoaded = 0
  val overallStart = System.currentTimeMillis // PROFILE

  /*
   * Each thread processing a SWAT partition starts by selecting a device to use
   * for the entirety of the processing for this partition. It then uses this
   * device to get a device context, which encapsulates all of the shared
   * resources (such as the clallocator, the broadcast cache, and any uncompiled
   * programs for this device).
   *
   * A SWAT context is then fetched, which is the combination of a particular
   * kernel and the device to run that kernel on. A SWAT context is
   * thread-unsafe and so is only accessible to a single thread at a time. It is
   * used to issue all commands and track all thread-local resources (such as
   * arguments collected for a particular kernel launch, and a compiled kernel
   * object for a specific device).
   */
  val partitionDeviceHint : Int = OpenCLBridge.getDeviceHintFor(
          rddId, partitionIndex, 0, 0)

 val deviceInitStart = System.currentTimeMillis // PROFILE
  val device_index = OpenCLBridge.getDeviceToUse(partitionDeviceHint,
          threadId, CLConfig.heapsPerDevice,
          CLConfig.heapSize, CLConfig.percHighPerfBuffers,
          false)
 System.err.println("Thread " + threadId + " selected device " + device_index) // PROFILE
  val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(device_index,
          CLConfig.heapsPerDevice, CLConfig.heapSize,
          CLConfig.percHighPerfBuffers, false)
  val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)
 RuntimeUtil.profPrint("DeviceInit", deviceInitStart, threadId) // PROFILE

  var firstBufferOp : Boolean = true
  var sampleOutput : java.lang.Object = None

  val initializing : Boolean = (entryPoint == null)

  if (initializing) {
    val initStart = System.currentTimeMillis // PROFILE
    // Configure code generation to target the appropriate language
    BlockWriter.emitOcl = (OpenCLBridge.usingCuda() == 0)

    sampleOutput = userLambda(firstSample).asInstanceOf[java.lang.Object]
    val entrypointAndKernel : Tuple2[Entrypoint, String] =
        if (!isAsyncMap) RuntimeUtil.getEntrypointAndKernel[T, U](firstSample,
            sampleOutput, params, userLambda, classModel, descriptor, dev_ctx,
            threadId, CLConfig.kernelDir, CLConfig.printKernel)
        else RuntimeUtil.getEntrypointAndKernel[U](sampleOutput, params,
            actualLambda.asInstanceOf[Function0[U]], classModel, descriptor,
            dev_ctx, threadId, CLConfig.kernelDir, CLConfig.printKernel)
    entryPoint = entrypointAndKernel._1
    openCL = entrypointAndKernel._2

    if (inputBuffer == null) {
      inputBuffer = RuntimeUtil.getInputBufferForSample(firstSample, CLConfig.N,
              DenseVectorInputBufferWrapperConfig.tiling,
              SparseVectorInputBufferWrapperConfig.tiling,
              PrimitiveArrayInputBufferWrapperConfig.tiling,
              entryPoint, false, isAsyncMap)

      chunkedOutputBuffer = OpenCLBridgeWrapper.getOutputBufferFor[U](
              sampleOutput.asInstanceOf[U], CLConfig.N,
              entryPoint, devicePointerSize, CLConfig.heapSize)
    }

    RuntimeUtil.profPrint("Initialization", initStart, threadId) // PROFILE
  }

  val outArgNum : Int = inputBuffer.countArgumentsUsed
  /*
   * When used as an output, only Tuple2 uses two arguments. All other types
   * use one.
   */
  val nOutArgs : Int = if (sampleOutput.isInstanceOf[Tuple2[_, _]] || sampleOutput.isInstanceOf[Array[_]]) 2 else 1
  var heapArgStart : Int = -1
  var lastArgIndex : Int = -1
  var heapTopArgNum : Int = -1

  val ctxCreateStart = System.currentTimeMillis // PROFILE
  val mySwatContextCache : PerThreadCache[KernelDevicePair, Long] =
      CLConfig.swatContextCache.forThread(threadId)

  val kernelDeviceKey : KernelDevicePair = new KernelDevicePair(
          actualLambda.getClass.getName, dev_ctx)
  if (!mySwatContextCache.hasAny(kernelDeviceKey)) {
    val ctx : Long = OpenCLBridge.createSwatContext(
              actualLambda.getClass.getName, openCL, dev_ctx, threadId,
              entryPoint.requiresDoublePragma,
              entryPoint.requiresHeap, CLConfig.N)
    mySwatContextCache.add(kernelDeviceKey, ctx)
  }
  val ctx : Long = mySwatContextCache.get(kernelDeviceKey)
  OpenCLBridge.resetSwatContext(ctx)

  for (i <- 0 until CLConfig.nNativeInputBuffers) {
    val newBuffer : NativeInputBuffers[T] = inputBuffer.generateNativeInputBuffer(dev_ctx)
    newBuffer.id = i
    initiallyEmptyNativeInputBuffers.add(newBuffer)
    nativeInputBuffersArray(i) = newBuffer
  }

  for (i <- 0 until CLConfig.nNativeOutputBuffers) {
    val newBuffer : NativeOutputBuffers[U] =
        chunkedOutputBuffer.generateNativeOutputBuffer(CLConfig.N,
                outArgNum, dev_ctx, ctx, sampleOutput.asInstanceOf[U], entryPoint)
    newBuffer.id = i
    emptiedNativeOutputBuffers.add(newBuffer)
    nativeOutputBuffersArray(i) = newBuffer
  }

  var argnum = outArgNum + nOutArgs
  
  if (!isAsyncMap) {
    val iter = entryPoint.getReferencedClassModelFields.iterator
    while (iter.hasNext) {
      val field = iter.next
      val isBroadcast = entryPoint.isBroadcastField(field)
      argnum += OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, argnum, actualLambda,
          field.getName, field.getDescriptor, entryPoint, isBroadcast)
    }
  }

  if (entryPoint.requiresHeap) {
    heapArgStart = argnum
    heapTopArgNum = heapArgStart + 1
    lastArgIndex = heapArgStart + 4
  } else {
    lastArgIndex = argnum
  }
  /*
   * Flush out the kernel arguments that never change, broadcast variables and
   * closure-captured variables
   */
  OpenCLBridge.setupGlobalArguments(ctx, dev_ctx) 

     RuntimeUtil.profPrint("ContextCreation", ctxCreateStart, threadId) // PROFILE
  var curr_kernel_ctx : Long = 0L
  var currentNativeOutputBuffer : NativeOutputBuffers[U] = null
  var curr_seq_no : Int = 0

  /*
   * Used when a new lambda is passed which has the same code, but captures
   * different data structures in its closure.
   */
  def resetGlobalArguments(newUserLambda : T => U) {
    OpenCLBridge.cleanupGlobalArguments(ctx, dev_ctx)
    val iter = entryPoint.getReferencedClassModelFields.iterator
    var localArgnum = outArgNum + nOutArgs
    while (iter.hasNext) {
      val field = iter.next
      val isBroadcast = entryPoint.isBroadcastField(field)
      localArgnum += OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, localArgnum,
          newUserLambda, field.getName, field.getDescriptor, entryPoint,
          isBroadcast)
    }

    OpenCLBridge.setupGlobalArguments(ctx, dev_ctx) 
  }

  def handleFullInputBuffer() : Tuple2[Int, NativeInputBuffers[T]] = {
    inputBuffer.flush

    val nLoaded = inputBuffer.nBuffered
    totalNLoaded += nLoaded

    val nextNativeInputBuffer : NativeInputBuffers[T] =
        if (!initiallyEmptyNativeInputBuffers.isEmpty) {
          initiallyEmptyNativeInputBuffers.remove
        } else {
          val id : Int = OpenCLBridge.waitForFreedNativeBuffer(ctx, dev_ctx)
          nativeInputBuffersArray(id)
        }

    inputBuffer.setupNativeBuffersForCopy(-1)
    val filled : NativeInputBuffers[T] = inputBuffer.transferOverflowTo(
            nextNativeInputBuffer)
    assert(filled.id != nextNativeInputBuffer.id)

    // Transfer input to device asynchronously
    if (filled.clBuffersReadyPtr != 0L) {
        OpenCLBridge.waitOnBufferReady(filled.clBuffersReadyPtr)
    }
    filled.copyToDevice(0, ctx, dev_ctx, NoCache, false)
    /*
     * Add a callback to notify the reader thread that a native input
     * buffer is now available
     */
    OpenCLBridge.enqueueBufferFreeCallback(ctx, dev_ctx, filled.id)

    currentNativeOutputBuffer =
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
              CLConfig.N * 4, false)) {
        throw new OpenCLOutOfMemoryException();
      }
    }

    OpenCLBridge.setIntArg(ctx, lastArgIndex, nLoaded)

    (nLoaded, filled)
  }

  override def next() : U = {
    if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
      if (curr_kernel_ctx > 0L) {
        System.err.println("SWAT PROF " + threadId + " Finished writing seq = " + // PROFILE
                (curr_seq_no - 1) + " @ " + System.currentTimeMillis) // PROFILE
        assert(currentNativeOutputBuffer != null)
        OpenCLBridge.cleanupKernelContext(curr_kernel_ctx)
        emptiedNativeOutputBuffers.synchronized {
          emptiedNativeOutputBuffers.add(currentNativeOutputBuffer)
          emptiedNativeOutputBuffers.notify
        }
      }
      curr_kernel_ctx = OpenCLBridge.waitForFinishedKernel(ctx, dev_ctx,
              curr_seq_no)
      System.err.println("SWAT PROF " + threadId + " Started writing seq = " + // PROFILE
              curr_seq_no + " @ " + System.currentTimeMillis) // PROFILE
      curr_seq_no += 1

      val current_output_buffer_id =
              OpenCLBridge.getOutputBufferIdFromKernelCtx(curr_kernel_ctx)
      currentNativeOutputBuffer = nativeOutputBuffersArray(
              current_output_buffer_id)

      chunkedOutputBuffer.fillFrom(curr_kernel_ctx, currentNativeOutputBuffer)

      outputBuffer = Some(chunkedOutputBuffer)
    }

    outputBuffer.get.next
  }

  var lastSeqNo : Int = -1

  def setLastSeqNo(set : Int) {
    assert(lastSeqNo == -1)
    lastSeqNo = set
  }

  def cleanup() {
    for (buffer <- nativeInputBuffersArray) {
      buffer.releaseOpenCLArrays
    }
    for (buffer <- nativeOutputBuffersArray) {
      buffer.releaseOpenCLArrays
    }

    OpenCLBridge.cleanupKernelContext(curr_kernel_ctx)
    OpenCLBridge.cleanupSwatContext(ctx, dev_ctx, context.stageId,
            context.partitionId)

    val mySwatContextCache : PerThreadCache[KernelDevicePair, Long] =
        CLConfig.swatContextCache.forThread(threadId)
    val kernelDeviceKey : KernelDevicePair = new KernelDevicePair(
            actualLambda.getClass.getName, dev_ctx)
    mySwatContextCache.add(kernelDeviceKey, ctx)

    val myInputBufferCache : PerThreadCache[String, InputBufferWrapper[_]] =
      CLConfig.inputBufferCache.forThread(threadId)
    val myOutputBufferCache : PerThreadCache[String, OutputBufferWrapper[_]] =
      CLConfig.outputBufferCache.forThread(threadId)
    myInputBufferCache.add(bufferKey, inputBuffer)
    myOutputBufferCache.add(bufferKey, chunkedOutputBuffer)
    inputBuffer = null
    chunkedOutputBuffer = null

    RuntimeUtil.profPrint("Total", overallStart, threadId) // PROFILE
    System.err.println("SWAT PROF Total loaded = " + totalNLoaded) // PROFILE
  }

  def hasNext() : Boolean = {
    /*
     * hasNext may be called multiple times after running out of items, in
     * which case inputBuffer may already be null on entry to this function.
     */
    val haveNext = inputBuffer != null && (lastSeqNo == -1 ||
            curr_seq_no <= lastSeqNo || outputBuffer.get.hasNext)
    if (!haveNext && inputBuffer != null) {

      System.err.println("SWAT PROF " + threadId + " Finished writing seq = " + // PROFILE
              (curr_seq_no - 1) + " @ " + System.currentTimeMillis) // PROFILE

      cleanup()
    }
    haveNext
  }
}
