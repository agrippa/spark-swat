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

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
    extends RDD[U](prev) {
  var knowType : Boolean = false
  var entryPoint : Entrypoint = null
  var openCL : String = null
  var ctx : Long = -1L
  var dev_ctx : Long = -1L
  val profile : Boolean = true

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

  override def compute(split: Partition, context: TaskContext) = {
    val N = 1024
    val acc : Array[T] = new Array[T](N)
    val output : Array[U] = new Array[U](N)

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

      var index = 0
      var nLoaded = 0
      var totalNLoaded = 0
      val overallStart = System.currentTimeMillis

      def next() : U = {
        if (index >= nLoaded) {
          assert(nested.hasNext)

          index = 0
          nLoaded = 0
          while (nLoaded < N && nested.hasNext) {
            acc(nLoaded) = nested.next
            nLoaded = nLoaded + 1
          }
          totalNLoaded += nLoaded

          if (!knowType && nLoaded > 0) {
            if (acc(0).isInstanceOf[Tuple2[_, _]]) {
              createHardCodedClassModel(acc(0).asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(0))
            }
            sampleOutput = f(acc(0)).asInstanceOf[java.lang.Object]
            if (sampleOutput.isInstanceOf[Tuple2[_, _]]) {
              createHardCodedClassModel(sampleOutput.asInstanceOf[Tuple2[_, _]],
                  hardCodedClassModels, params.get(1))
            }
            knowType = true
          }

          if (entryPoint == null) {
            var genStart : Long = 0
            if (profile) genStart = System.currentTimeMillis

            entryPoint = classModel.getEntrypoint("apply", descriptor,
                f, params, hardCodedClassModels);

            val writerAndKernel = KernelWriter.writeToString(
                entryPoint, params)
            openCL = writerAndKernel.kernel
            // System.err.println(openCL)

            var initStart : Long = 0
            if (profile) {
              System.err.println("SWAT PROF CodeGeneration " +
                  (System.currentTimeMillis - genStart) + " ms")
              initStart = System.currentTimeMillis
            }

            val threadNamePrefix = "Executor task launch worker-"
            val threadName = Thread.currentThread.getName
            if (!threadName.startsWith(threadNamePrefix)) {
                throw new RuntimeException("Unexpected thread name \"" + threadName + "\"")
            }
            val threadId : Int = Integer.parseInt(threadName.substring(threadNamePrefix.length))

            dev_ctx = OpenCLBridge.getDeviceContext(threadId)
            ctx = OpenCLBridge.createSwatContext(f.getClass.getName, openCL, dev_ctx, threadId,
                entryPoint.requiresDoublePragma, entryPoint.requiresHeap);
            if (profile) {
              System.err.println("SWAT PROF Initialization " +
                  (System.currentTimeMillis - initStart) + " ms")
            }
          }

          var writeStart, runStart, readStart : Long = 0
          if (profile) writeStart = System.currentTimeMillis
          var argnum : Int = 0
          argnum = argnum + OpenCLBridgeWrapper.setArrayArg[T](ctx, dev_ctx, 0, acc,
                  nLoaded, true, entryPoint)
          val outArgNum : Int = argnum
          argnum = argnum + OpenCLBridgeWrapper.setUnitializedArrayArg[U](ctx,
              dev_ctx, argnum, output.size, classTag[U].runtimeClass,
              entryPoint, sampleOutput.asInstanceOf[U])

          val iter = entryPoint.getReferencedClassModelFields.iterator
          while (iter.hasNext) {
            val field = iter.next
            val isBroadcast = entryPoint.isBroadcastField(field)
            argnum = argnum + OpenCLBridge.setArgByNameAndType(ctx, dev_ctx, argnum, f,
                field.getName, field.getDescriptor, entryPoint, isBroadcast)
          }

          val heapArgStart : Int = argnum
          if (entryPoint.requiresHeap) {
            argnum = argnum + OpenCLBridge.createHeap(ctx, dev_ctx, argnum, 100 * 1024 * 1024, nLoaded)
          }   

          OpenCLBridge.setIntArg(ctx, argnum, nLoaded)

          if (profile) {
            System.err.println("SWAT PROF Write " +
                (System.currentTimeMillis - writeStart) + " ms")
            runStart = System.currentTimeMillis
          }
          if (entryPoint.requiresHeap) {
            val anyFailed : Array[Int] = new Array[Int](1)
            do {
              OpenCLBridge.run(ctx, dev_ctx, nLoaded);
              OpenCLBridgeWrapper.fetchArrayArg(ctx, dev_ctx, argnum - 1, anyFailed, entryPoint)
              OpenCLBridge.resetHeap(ctx, dev_ctx, heapArgStart)
            } while (anyFailed(0) > 0)
          } else {
            OpenCLBridge.run(ctx, dev_ctx, nLoaded);
          }

          if (profile) {
            System.err.println("SWAT PROF Run " +
                (System.currentTimeMillis - runStart) + " ms")
            readStart = System.currentTimeMillis
          }
          OpenCLBridgeWrapper.fetchArgFromUnitializedArray[U](ctx, dev_ctx, outArgNum,
              output, entryPoint, sampleOutput.asInstanceOf[U])

          if (profile) {
            System.err.println("SWAT PROF Read " +
                (System.currentTimeMillis - readStart) + " ms")
          }
        }

        val curr = index
        index = index + 1
        output(curr)
      }

      def hasNext : Boolean = {
        val nonEmpty = (index < nLoaded || nested.hasNext)
        if (!nonEmpty) {
          OpenCLBridge.cleanupSwatContext(ctx)
          if (profile) {
            System.err.println("SWAT PROF Processed " + totalNLoaded +
                " elements")
            System.err.println("SWAT PROF Total " +
                (System.currentTimeMillis - overallStart) + " ms")
          }
        }
        nonEmpty
      }
    }
    iter
  }

  override def map[V: ClassTag](f: U => V): RDD[V] = {
    new CLMappedRDD(this, sparkContext.clean(f))
  }
}
