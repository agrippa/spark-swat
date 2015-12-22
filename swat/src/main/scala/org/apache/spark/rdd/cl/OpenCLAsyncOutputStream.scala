package org.apache.spark.rdd.cl

import java.util.LinkedList
import java.lang.reflect.Field

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.writer.ScalaArrayParameter

class OpenCLAsyncOutputStream[U: ClassTag](val multiOutput : Boolean,
    val dev_ctx : Long, val threadId : Int) extends AsyncOutputStream[U] {
  val tmpBuffer : java.util.LinkedList[Option[U]] = new java.util.LinkedList[Option[U]]
  var ctx : Long = 0

  val devicePointerSize = OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)

  var inputFields : Array[Field] = null
  var inputBuffers : Array[InputBufferWrapper[_]] = null
  var nativeInputBuffers : Array[NativeInputBuffers[_]] = null
  var inactiveNativeInputBuffers : Array[NativeInputBuffers[_]] = null
  var nativeOutputBuffer : NativeOutputBuffers[U] = null
  var chunkedOutputBuffer : OutputBufferWrapper[U] = null

  var nLoaded : Int = 0

  var lambdaName : String = null
  var entryPoint : Entrypoint = null
  var openCL : String = null

  var heapArgStart : Int = -1
  var lastArgIndex : Int = -1
  var heapTopArgNum : Int = -1

  var currSeqNo : Int = 0

  private def launch() {
    for (i <- 0 until inputFields.size) {
      val nextNativeInputBuffer = inactiveNativeInputBuffers(i)
      inputBuffers(i).setupNativeBuffersForCopy(-1)
      val filled = inputBuffers(i).transferOverflowTo(nextNativeInputBuffer)
      assert(filled.id != nextNativeInputBuffer.id)

      filled.copyToDevice(i + 1, ctx, dev_ctx, NoCache, false)
      nativeOutputBuffer.addToArgs
    }

    if (entryPoint.requiresHeap) {
      // processing_succeeded
      if (!OpenCLBridge.setArgUnitialized(ctx, dev_ctx, heapTopArgNum + 2,
              CLConfig.N * 4, false)) {
        throw new OpenCLOutOfMemoryException();
      }
    }

    OpenCLBridge.setIntArg(ctx, lastArgIndex, nLoaded)
    OpenCLBridge.run(ctx, dev_ctx, nLoaded,
            CLConfig.cl_local_size, lastArgIndex + 1,
            heapArgStart, CLConfig.heapsPerDevice,
            nativeOutputBuffer.id)
    nLoaded = 0

    val curr_kernel_ctx : Long = OpenCLBridge.waitForFinishedKernel(ctx, dev_ctx, currSeqNo)
    currSeqNo += 1
    chunkedOutputBuffer.fillFrom(curr_kernel_ctx, nativeOutputBuffer)

    var count : Int = 0
    while (chunkedOutputBuffer.hasNext) {
      val output = chunkedOutputBuffer.next
      count += 1
    }
  }

  override def spawn(l: () => U) {
    assert(l != null)

    val output : Option[U] = Some(l())

    val initializing : Boolean = (entryPoint == null)
    if (initializing) {
      lambdaName = l.getClass.getName

      val classModel : ClassModel = ClassModel.createClassModel(l.getClass, null,
          new ShouldNotCallMatcher())
      val method = classModel.getPrimitiveApplyMethod
      val descriptor : String = method.getDescriptor

      // 1 argument expected for maps
      val params : LinkedList[ScalaArrayParameter] = new LinkedList[ScalaArrayParameter]
      params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

      val entrypointAndKernel : Tuple2[Entrypoint, String] =
          RuntimeUtil.getEntrypointAndKernel[U](output.get.asInstanceOf[java.lang.Object],
          params, l, classModel, descriptor, dev_ctx, threadId,
          CLConfig.kernelDir, CLConfig.printKernel)
      entryPoint = entrypointAndKernel._1
      openCL = entrypointAndKernel._2

      ctx = OpenCLBridge.createSwatContext(
                l.getClass.getName, openCL, dev_ctx, threadId,
                entryPoint.requiresDoublePragma,
                entryPoint.requiresHeap, CLConfig.N)
      OpenCLBridge.resetSwatContext(ctx)

      val nReferencedFields = entryPoint.getReferencedClassModelFields.size
      inputFields = new Array[Field](nReferencedFields)
      inputBuffers = new Array[InputBufferWrapper[_]](nReferencedFields)
      nativeInputBuffers = new Array[NativeInputBuffers[_]](nReferencedFields)
      inactiveNativeInputBuffers = new Array[NativeInputBuffers[_]](nReferencedFields)
      var index = 0

      val iter = entryPoint.getReferencedClassModelFields.iterator
      while (iter.hasNext) {
        val field = iter.next
        val fieldName = field.getName
        val fieldDesc = field.getDescriptor

        val javaField : Field = l.getClass.getDeclaredField(fieldName)
        javaField.setAccessible(true)
        val fieldSample : java.lang.Object = javaField.get(l)

        inputFields(index) = javaField
        inputBuffers(index) = RuntimeUtil.getInputBufferForSample(fieldSample,
                CLConfig.N, DenseVectorInputBufferWrapperConfig.tiling,
                SparseVectorInputBufferWrapperConfig.tiling,
                entryPoint, false)

        nativeInputBuffers(index) = inputBuffers(index).generateNativeInputBuffer(dev_ctx)
        nativeInputBuffers(index).id = 0

        inactiveNativeInputBuffers(index) = inputBuffers(index).generateNativeInputBuffer(dev_ctx)
        inactiveNativeInputBuffers(index).id = 1

        inputBuffers(index).setCurrentNativeBuffers(nativeInputBuffers(index))
        inputBuffers(index).reset

        index += 1
      }

      chunkedOutputBuffer = OpenCLBridgeWrapper.getOutputBufferFor[U](
              output.get, CLConfig.N, entryPoint, devicePointerSize,
              CLConfig.heapSize)
      nativeOutputBuffer = chunkedOutputBuffer.generateNativeOutputBuffer(
              CLConfig.N, 0, dev_ctx, ctx, output.get, entryPoint)

      if (entryPoint.requiresHeap) {
        heapArgStart = nReferencedFields + 1
        heapTopArgNum = heapArgStart + 1
        lastArgIndex = heapArgStart + 4
      } else {
        lastArgIndex = nReferencedFields + 1
      }
    } else {
      assert(lambdaName == l.getClass.getName)
    }

    var anyOutOfSpace = false
    for (i <- 0 until inputFields.size) {
      val field = inputFields(i)
      val buf = inputBuffers(i)
      val value = field.get(l)

      buf.append(value)
      buf.flush

      if (buf.outOfSpace) anyOutOfSpace = true
    }
    nLoaded += 1

    if (anyOutOfSpace) {
      launch()
    }

    tmpBuffer.synchronized {
      tmpBuffer.add(output)
      tmpBuffer.notify
    }

    if (!multiOutput) {
      throw new SuspendException
    }
  }

  override def finish() {
    if (inputBuffers(0).nBuffered > 0) {
      launch()
    }
    tmpBuffer.synchronized {
      tmpBuffer.add(None)
      tmpBuffer.notify
    }
  }

  override def pop() : Option[U] = {
    var result : Option[U] = None

    tmpBuffer.synchronized {
      while (tmpBuffer.isEmpty) {
        tmpBuffer.wait
      }

      result = tmpBuffer.poll
    }

    result
  }
}
