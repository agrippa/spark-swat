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

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U,
    enableNested : Boolean) extends RDD[U](prev) {
  def this(prev: RDD[T], f: T => U) = this(prev, f, true)

  val heapSize = 100 * 1024 * 1024
  var entryPoint : Entrypoint = null
  var openCL : String = null
  // var ctx : Long = -1L
  val ctxCache : java.util.Map[Long, Long] = new java.util.HashMap[Long, Long]()
  // var dev_ctx : Long = -1L

  override val partitioner = None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  def createHardCodedDenseVectorClassModel(hardCodedClassModels : HardCodedClassModels) {
    val denseVectorClassModel : DenseVectorClassModel =
        DenseVectorClassModel.create(DenseVectorInputBufferWrapperConfig.tiling)
    hardCodedClassModels.addClassModelFor(
            Class.forName("org.apache.spark.mllib.linalg.DenseVector"),
            denseVectorClassModel)
  }

  def createHardCodedSparseVectorClassModel(hardCodedClassModels : HardCodedClassModels) {
    val sparseVectorClassModel : SparseVectorClassModel =
        SparseVectorClassModel.create(SparseVectorInputBufferWrapperConfig.tiling)
    hardCodedClassModels.addClassModelFor(
            Class.forName("org.apache.spark.mllib.linalg.SparseVector"),
            sparseVectorClassModel)
  }

  def createHardCodedTuple2ClassModel(obj : Tuple2[_, _],
      hardCodedClassModels : HardCodedClassModels,
      param : ScalaArrayParameter) {
    val inputClassType1 = obj._1.getClass
    val inputClassType2 = obj._2.getClass

    val inputClassType1Name = CodeGenUtil.cleanClassName(
        inputClassType1.getName)
    val inputClassType2Name = CodeGenUtil.cleanClassName(
        inputClassType2.getName)

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, param.getDir != DIRECTION.IN)
    hardCodedClassModels.addClassModelFor(Class.forName("scala.Tuple2"), tuple2ClassModel)

    param.addTypeParameter(inputClassType1Name,
        !CodeGenUtil.isPrimitive(inputClassType1Name))
    param.addTypeParameter(inputClassType2Name,
        !CodeGenUtil.isPrimitive(inputClassType2Name))
  }

  def profPrint(lbl : String, startTime : Long, threadId : Int) { // PROFILE
      System.err.println("SWAT PROF " + threadId + " " + lbl + " " + // PROFILE
          (System.currentTimeMillis - startTime) + " ms") // PROFILE
  } // PROFILE

  override def compute(split: Partition, context: TaskContext) = {
    // val N = sparkContext.getConf.get("swat.chunking").toInt
    val N = 65536 * 8
    var acc : Option[InputBufferWrapper[T]] = None
    var outputBuffer : Option[OutputBufferWrapper[U]] = None
    val bbCache : ByteBufferCache = new ByteBufferCache(2)

    val threadNamePrefix = "Executor task launch worker-"
    val threadName = Thread.currentThread.getName
    if (!threadName.startsWith(threadNamePrefix)) {
        throw new RuntimeException("Unexpected thread name \"" + threadName + "\"")
    }
    val threadId : Int = Integer.parseInt(threadName.substring(threadNamePrefix.length))

    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    System.setProperty("com.amd.aparapi.enable.ATHROW", "true");

    val classModel : ClassModel = ClassModel.createClassModel(f.getClass, null,
        new ShouldNotCallMatcher())
    val hardCodedClassModels : HardCodedClassModels = new HardCodedClassModels()
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
            if (firstSample.isInstanceOf[Tuple2[_, _]]) {
              createHardCodedTuple2ClassModel(firstSample.asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(0))
            } else if (firstSample.isInstanceOf[DenseVector]) {
              createHardCodedDenseVectorClassModel(hardCodedClassModels)
            } else if (firstSample.isInstanceOf[SparseVector]) {
              createHardCodedSparseVectorClassModel(hardCodedClassModels)
            }

            sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
            if (sampleOutput.isInstanceOf[Tuple2[_, _]]) {
              createHardCodedTuple2ClassModel(sampleOutput.asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(1))
            } else if (sampleOutput.isInstanceOf[DenseVector]) {
              createHardCodedDenseVectorClassModel(hardCodedClassModels)
            } else if (sampleOutput.isInstanceOf[SparseVector]) {
              createHardCodedSparseVectorClassModel(hardCodedClassModels)
            }

            val entrypointKey : EntrypointCacheKey = new EntrypointCacheKey(
                    f.getClass.getName, enableNested)
            EntrypointCache.cache.synchronized {
              if (EntrypointCache.cache.containsKey(entrypointKey)) {
                entryPoint = EntrypointCache.cache.get(entrypointKey)
              } else {
                entryPoint = classModel.getEntrypoint("apply", descriptor,
                    f, params, hardCodedClassModels,
                    CodeGenUtil.createCodeGenConfig(dev_ctx), enableNested)
                EntrypointCache.cache.put(entrypointKey, entryPoint)
              }

              if (EntrypointCache.kernelCache.containsKey(entrypointKey)) {
                openCL = EntrypointCache.kernelCache.get(entrypointKey)
              } else {
                val writerAndKernel = KernelWriter.writeToString(
                    entryPoint, params)
                openCL = writerAndKernel.kernel
                // System.err.println(openCL)
                EntrypointCache.kernelCache.put(entrypointKey, openCL)
              }
            }

            if (firstSample.isInstanceOf[Double] ||
                firstSample.isInstanceOf[Int] ||
                firstSample.isInstanceOf[Float]) {
              acc = Some(new PrimitiveInputBufferWrapper(N))
            } else if (firstSample.isInstanceOf[Tuple2[_, _]]) {
              acc = Some(new Tuple2InputBufferWrapper(N,
                          firstSample.asInstanceOf[Tuple2[_, _]],
                          entryPoint).asInstanceOf[InputBufferWrapper[T]])
            } else if (firstSample.isInstanceOf[DenseVector]) {
              acc = Some(new DenseVectorInputBufferWrapper(N,
                          entryPoint).asInstanceOf[InputBufferWrapper[T]])
            } else if (firstSample.isInstanceOf[SparseVector]) {
              acc = Some(new SparseVectorInputBufferWrapper(N,
                          entryPoint).asInstanceOf[InputBufferWrapper[T]])
            } else {
              acc = Some(new ObjectInputBufferWrapper(N,
                          firstSample.getClass.getName, entryPoint))
            }
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
            OpenCLBridge.run(ctx, dev_ctx, nLoaded,
                    entryPoint.checkIsWorkSharingKernel)
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

object EntrypointCache {
  val cache : java.util.Map[EntrypointCacheKey, Entrypoint] =
      new java.util.HashMap[EntrypointCacheKey, Entrypoint]()
  val kernelCache : java.util.Map[EntrypointCacheKey, java.lang.String] =
      new java.util.HashMap[EntrypointCacheKey, java.lang.String]()
}

class EntrypointCacheKey(className : java.lang.String, enableNested : Boolean)
    extends java.lang.Comparable[EntrypointCacheKey] {
  def getClassName() : java.lang.String = { className }
  def getEnableNested() : Boolean = { enableNested }

  def compareTo(other : EntrypointCacheKey) : Int = {
    if (!className.equals(other.getClassName)) {
      return className.compareTo(other.getClassName)
    } else {
      if (enableNested == other.getEnableNested) {
        return 0
      } else if (!enableNested && other.getEnableNested) {
        return -1
      } else {
        return 1
      }
    }
  }
}
