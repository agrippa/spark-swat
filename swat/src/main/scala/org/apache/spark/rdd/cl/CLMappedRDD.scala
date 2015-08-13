package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

import java.net._
import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter.DIRECTION

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U, cl_id : Int)
    extends RDD[U](prev) {
  var entryPoint : Entrypoint = null
  var openCL : String = null
  var ctx : Long = -1L
  var dev_ctx : Long = -1L
  val profile : Boolean = false

  override val partitioner = None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  def createHardCodedClassModel(obj : Tuple2[_, _],
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

  def profPrint(lbl : String, startTime : Long, threadId : Int) {
      System.err.println("SWAT PROF " + threadId + " " + lbl + " " +
          (System.currentTimeMillis - startTime) + " ms")
  }

  override def compute(split: Partition, context: TaskContext) = {
    // val N = sparkContext.getConf.get("swat.chunking").toInt
    val N = 65536 * 16
    // val acc : Array[T] = new Array[T](N)
    var acc : Option[InputBufferWrapper[T]] = None
    var outputBuffer : Option[OutputBufferWrapper[U]] = None
    val bbCache : ByteBufferCache = new ByteBufferCache(2)

    val threadNamePrefix = "Executor task launch worker-"
    val threadName = Thread.currentThread.getName
    if (!threadName.startsWith(threadNamePrefix)) {
        throw new RuntimeException("Unexpected thread name \"" + threadName + "\"")
    }
    val threadId : Int = Integer.parseInt(threadName.substring(threadNamePrefix.length))

    // System.err.println("SWAT Hello from Thread " +
    //     Thread.currentThread().getName() + ", attemptId=" + context.attemptId +
    //     ", partitionId=" + context.partitionId + " stageId=" + context.stageId +
    //     " runningLocally=" + context.runningLocally);

    System.setProperty("com.amd.aparapi.enable.NEW", "true");
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

      if (profile && threadId == 1) {
          System.err.println("compute called from:")
          for (trace <- Thread.currentThread.getStackTrace) {
            System.err.println("  " + trace.toString)
          }
      }

      var totalNLoaded = 0
      val overallStart = System.currentTimeMillis

      def next() : U = {

        if (outputBuffer.isEmpty || !outputBuffer.get.hasNext) {
          assert(nested.hasNext)

          if (!outputBuffer.isEmpty) {
            outputBuffer.get.releaseBuffers(bbCache)
          }

          var ioStart : Long = 0
          if (profile) ioStart = System.currentTimeMillis
          val firstSample : T = nested.next

          if (entryPoint == null) {
            var genStart, initStart : Long = 0
            if (profile) genStart = System.currentTimeMillis

            if (firstSample.isInstanceOf[Tuple2[_, _]]) {
              createHardCodedClassModel(firstSample.asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(0))
            }
            sampleOutput = f(firstSample).asInstanceOf[java.lang.Object]
            if (sampleOutput.isInstanceOf[Tuple2[_, _]]) {
              createHardCodedClassModel(sampleOutput.asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(1))
            }

            EntrypointCache.cache.synchronized {
              if (EntrypointCache.cache.containsKey(f.getClass.getName)) {
                entryPoint = EntrypointCache.cache.get(f.getClass.getName)
              } else {
                entryPoint = classModel.getEntrypoint("apply", descriptor,
                    f, params, hardCodedClassModels);
                EntrypointCache.cache.put(f.getClass.getName, entryPoint)
              }
            }

            val writerAndKernel = KernelWriter.writeToString(
                entryPoint, params)
            openCL = writerAndKernel.kernel
            // System.err.println(openCL)

            if (profile) {
              profPrint("CodeGeneration", genStart, threadId)
              initStart = System.currentTimeMillis
            }

            dev_ctx = OpenCLBridge.getDeviceContext(threadId)
            ctx = OpenCLBridge.createSwatContext(f.getClass.getName, openCL, dev_ctx, threadId,
                entryPoint.requiresDoublePragma, entryPoint.requiresHeap);
            if (profile) {
              profPrint("Initialization", initStart, threadId)
            }

            if (firstSample.isInstanceOf[Double] ||
                firstSample.isInstanceOf[Int] ||
                firstSample.isInstanceOf[Float]) {
              acc = Some(new PrimitiveInputBufferWrapper(N))
            } else if (firstSample.isInstanceOf[Tuple2[_, _]]) {
              acc = Some(new Tuple2InputBufferWrapper(N, firstSample.asInstanceOf[Tuple2[_, _]], entryPoint).asInstanceOf[InputBufferWrapper[T]])
            } else {
              acc = Some(new ObjectInputBufferWrapper(N, firstSample.getClass.getName, entryPoint))
            }
          }

          var nLoaded = 1
          acc.get.append(firstSample)
          while (nLoaded < N && nested.hasNext) {
            acc.get.append(nested.next)
            nLoaded = nLoaded + 1
          }
          val myOffset : Int = totalNLoaded
          totalNLoaded += nLoaded

          if (profile) {
            profPrint("Input-I/O", ioStart, threadId)
          }

          var writeStart, runStart, readStart, postStart : Long = 0
          if (profile) writeStart = System.currentTimeMillis
          var argnum : Int = acc.get.copyToDevice(0, ctx, dev_ctx, cl_id,
                  split.index, myOffset)
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
            argnum += OpenCLBridge.createHeap(ctx, dev_ctx, argnum, 100 * 1024 * 1024, nLoaded)
          }   

          OpenCLBridge.setIntArg(ctx, argnum, nLoaded)

          if (profile) {
            profPrint("Write", writeStart, threadId)
            runStart = System.currentTimeMillis
          }
          if (entryPoint.requiresHeap) {
            val anyFailed : Array[Int] = new Array[Int](1)
            var retries : Int = 0
            do {
              OpenCLBridge.run(ctx, dev_ctx, nLoaded);
              OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, argnum - 1,
                  anyFailed, entryPoint, bbCache)
              OpenCLBridge.resetHeap(ctx, dev_ctx, heapArgStart)
              retries += 1
            } while (anyFailed(0) > 0)
          } else {
            OpenCLBridge.run(ctx, dev_ctx, nLoaded);
          }

          if (profile) {
            profPrint("Run", runStart, threadId)
            readStart = System.currentTimeMillis
          }
          outputBuffer = Some(OpenCLBridgeWrapper.fetchArgFromUnitializedArray[U](ctx, dev_ctx, outArgNum,
              nLoaded, entryPoint, sampleOutput.asInstanceOf[U], bbCache))

          if (profile) {
            profPrint("Read", readStart, threadId)
            postStart = System.currentTimeMillis
          }
          OpenCLBridge.postKernelCleanup(ctx);
          if (profile) {
            profPrint("Post", postStart, threadId)
          }
        }

        outputBuffer.get.next
      }

      def hasNext : Boolean = {
        val nonEmpty = (nested.hasNext || outputBuffer.get.hasNext)
        if (!nonEmpty) {
          OpenCLBridge.cleanupSwatContext(ctx)
          if (profile) {
            System.err.println("SWAT PROF " + threadId + " Processed " + totalNLoaded +
                " elements")
            profPrint("Total", overallStart, threadId)
          }
        }
        nonEmpty
      }
    }
    iter
  }

  override def map[V: ClassTag](f: U => V): RDD[V] = {
    new CLMappedRDD(this, sparkContext.clean(f), CLWrapper.counter.getAndAdd(1))
  }
}

object EntrypointCache {
  val cache : java.util.Map[java.lang.String, Entrypoint] =
      new java.util.HashMap[java.lang.String, Entrypoint]()
}
