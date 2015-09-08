package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector

import java.util.LinkedList

import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.SparseVectorClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.writer.ScalaParameter
import com.amd.aparapi.internal.writer.ScalaParameter.DIRECTION
import com.amd.aparapi.internal.model.Entrypoint

object RuntimeUtil {
  def getEntrypointAndKernel[T: ClassTag, U: ClassTag](firstSample : T,
      sampleOutput : java.lang.Object,
      params : LinkedList[ScalaArrayParameter], lambda : T => U,
      classModel : ClassModel, methodDescriptor : String, dev_ctx : Long) :
      Tuple2[Entrypoint, String] = {
    var entryPoint : Entrypoint = null
    var openCL : String = null

    val hardCodedClassModels : HardCodedClassModels = new HardCodedClassModels()
    if (firstSample.isInstanceOf[Tuple2[_, _]]) {
      CodeGenUtil.createHardCodedTuple2ClassModel(firstSample.asInstanceOf[Tuple2[_, _]],
          hardCodedClassModels, params.get(0))
    } else if (firstSample.isInstanceOf[DenseVector]) {
      CodeGenUtil.createHardCodedDenseVectorClassModel(hardCodedClassModels)
    } else if (firstSample.isInstanceOf[SparseVector]) {
      CodeGenUtil.createHardCodedSparseVectorClassModel(hardCodedClassModels)
    }

    if (sampleOutput != null) {
      if (sampleOutput.isInstanceOf[Tuple2[_, _]]) {
        CodeGenUtil.createHardCodedTuple2ClassModel(sampleOutput.asInstanceOf[Tuple2[_, _]],
            hardCodedClassModels, params.get(1))
      } else if (sampleOutput.isInstanceOf[DenseVector]) {
        CodeGenUtil.createHardCodedDenseVectorClassModel(hardCodedClassModels)
      } else if (sampleOutput.isInstanceOf[SparseVector]) {
        CodeGenUtil.createHardCodedSparseVectorClassModel(hardCodedClassModels)
      }
    }

    val entrypointKey : EntrypointCacheKey = new EntrypointCacheKey(
            lambda.getClass.getName)
    EntrypointCache.cache.synchronized {
      if (EntrypointCache.cache.containsKey(entrypointKey)) {
        System.err.println("using cached entrypoint")
        entryPoint = EntrypointCache.cache.get(entrypointKey)
      } else {
        System.err.println("computing entrypoint")
        entryPoint = classModel.getEntrypoint("apply", methodDescriptor,
            lambda, params, hardCodedClassModels,
            CodeGenUtil.createCodeGenConfig(dev_ctx))
        EntrypointCache.cache.put(entrypointKey, entryPoint)
      }

      if (EntrypointCache.kernelCache.containsKey(entrypointKey)) {
        System.err.println("using cached kernel")
        openCL = EntrypointCache.kernelCache.get(entrypointKey)
      } else {
        System.err.println("computing kernel")
        val writerAndKernel = KernelWriter.writeToString(
            entryPoint, params)
        openCL = writerAndKernel.kernel
        // System.err.println(openCL)
        EntrypointCache.kernelCache.put(entrypointKey, openCL)
      }
    }
    (entryPoint, openCL)
  }

  def getThreadID() : Int = {
    val threadNamePrefix = "Executor task launch worker-"
    val threadName = Thread.currentThread.getName
    if (!threadName.startsWith(threadNamePrefix)) {
        throw new RuntimeException("Unexpected thread name \"" + threadName + "\"")
    }
    Integer.parseInt(threadName.substring(threadNamePrefix.length))
  }

  def getInputBufferFor[T : ClassTag](firstSample : T, N : Int,
      entryPoint : Entrypoint) : InputBufferWrapper[T] = {
    if (firstSample.isInstanceOf[Double] ||
        firstSample.isInstanceOf[Int] ||
        firstSample.isInstanceOf[Float]) {
      new PrimitiveInputBufferWrapper(N)
    } else if (firstSample.isInstanceOf[Tuple2[_, _]]) {
      new Tuple2InputBufferWrapper(N,
              firstSample.asInstanceOf[Tuple2[_, _]],
              entryPoint).asInstanceOf[InputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[DenseVector]) {
      new DenseVectorInputBufferWrapper(N,
              entryPoint).asInstanceOf[InputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[SparseVector]) {
      new SparseVectorInputBufferWrapper(N,
              entryPoint).asInstanceOf[InputBufferWrapper[T]]
    } else {
      new ObjectInputBufferWrapper(N,
              firstSample.getClass.getName, entryPoint)
    }
  }
}
