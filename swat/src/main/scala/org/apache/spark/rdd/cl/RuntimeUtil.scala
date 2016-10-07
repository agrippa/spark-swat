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
import scala.io.Source
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
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.Entrypoint

object RuntimeUtil {

  def profPrint(lbl : String, startTime : Long, threadId : Int) { // PROFILE
      profPrintTotal(lbl, System.currentTimeMillis - startTime, threadId) // PROFILE
  } // PROFILE

  def profPrintTotal(lbl : String, totalTime : Long, threadId : Int) { // PROFILE
      System.err.println("SWAT PROF " + threadId + " " + lbl + " " + // PROFILE
          totalTime + " ms " + System.currentTimeMillis) // PROFILE
  } // PROFILE

  def createHardCodedClassModelsForSampleOutput(sampleOutput : java.lang.Object,
      hardCodedClassModels : HardCodedClassModels,
      params : LinkedList[ScalaArrayParameter]) {
    if (sampleOutput != null) {
      if (sampleOutput.isInstanceOf[Tuple2[_, _]]) {
        CodeGenUtil.createHardCodedTuple2ClassModel(
            sampleOutput.asInstanceOf[Tuple2[_, _]], hardCodedClassModels,
            params.get(params.size - 1))
      } else if (sampleOutput.isInstanceOf[DenseVector]) {
        CodeGenUtil.createHardCodedDenseVectorClassModel(hardCodedClassModels)
      } else if (sampleOutput.isInstanceOf[SparseVector]) {
        CodeGenUtil.createHardCodedSparseVectorClassModel(hardCodedClassModels)
      }
    }
  }

  private def getEntrypointAndKernelHelper(
      hardCodedClassModels : HardCodedClassModels, lambda : java.lang.Object,
      classModel : ClassModel, methodDescriptor : String,
      params : LinkedList[ScalaArrayParameter], dev_ctx : Long,
      kernelDir : String, printKernel : Boolean, multiInput : Boolean) : Tuple2[Entrypoint, String] = {
    var entryPoint : Entrypoint = null
    var openCL : String = null

    val entrypointKey : EntrypointCacheKey = new EntrypointCacheKey(
            lambda.getClass.getName)
    EntrypointCache.cache.synchronized {
      if (EntrypointCache.cache.containsKey(entrypointKey)) {
//         System.err.println("Thread " + threadId + " using cached entrypoint") // PROFILE
        entryPoint = EntrypointCache.cache.get(entrypointKey)
      } else {
//         System.err.println("Thread " + threadId + " generating entrypoint") // PROFILE
        entryPoint = classModel.getEntrypoint("apply", methodDescriptor,
            lambda, params, hardCodedClassModels,
            CodeGenUtil.createCodeGenConfig(dev_ctx))
        EntrypointCache.cache.put(entrypointKey, entryPoint)
      }

//       profPrint("EntrypointGeneration", startEntrypointGeneration, threadId) // PROFILE
//       val startKernelGeneration = System.currentTimeMillis // PROFILE

      if (EntrypointCache.kernelCache.containsKey(entrypointKey)) {
//         System.err.println("Thread " + threadId + " using cached kernel") // PROFILE
        openCL = EntrypointCache.kernelCache.get(entrypointKey)
      } else {
//         System.err.println("Thread " + threadId + " generating kernel") // PROFILE
        val writerAndKernel = KernelWriter.writeToString(
            entryPoint, params, multiInput)
        openCL = writerAndKernel.kernel

        val kernelFilename = kernelDir + "/" + lambda.getClass.getName + ".kernel"
        val handCodedKernel = try {
            System.out.println("Loading kernel for " + lambda.getClass.getName +
                    " from " + kernelFilename)
            Source.fromFile(kernelFilename).getLines.mkString
        } catch {
            case fnf: java.io.FileNotFoundException => null
            case ex: Exception => throw ex
        }
        if (handCodedKernel != null) {
            openCL = handCodedKernel
        }

        if (printKernel) {
          System.err.println("Kernel name = " + lambda.getClass.getName)
          System.err.println(openCL)
        }
        EntrypointCache.kernelCache.put(entrypointKey, openCL)
      }

//       profPrint("KernelGeneration", startKernelGeneration, threadId) // PROFILE
    }

    (entryPoint, openCL)
  }

  def getEntrypointAndKernel[U](sampleOutput : java.lang.Object,
      params : LinkedList[ScalaArrayParameter], lambda : () => U,
      classModel : ClassModel, methodDescriptor : String,
      dev_ctx : Long, threadId : Int, kernelDir : String,
      printKernel : Boolean) : Tuple2[Entrypoint, String] = {
//     val startClassGeneration = System.currentTimeMillis // PROFILE

    val hardCodedClassModels : HardCodedClassModels = new HardCodedClassModels()
    createHardCodedClassModelsForSampleOutput(sampleOutput,
            hardCodedClassModels, params)

//     profPrint("HardCodedClassGeneration", startClassGeneration, threadId) // PROFILE
//     val startEntrypointGeneration = System.currentTimeMillis // PROFILE

    getEntrypointAndKernelHelper(hardCodedClassModels,
            lambda.asInstanceOf[java.lang.Object], classModel, methodDescriptor,
            params, dev_ctx, kernelDir, printKernel, true)
  }

  def getEntrypointAndKernel[T: ClassTag, U](firstSample : T,
      sampleOutput : java.lang.Object, params : LinkedList[ScalaArrayParameter],
      lambda : T => U, classModel : ClassModel, methodDescriptor : String,
      dev_ctx : Long, threadId : Int, kernelDir : String,
      printKernel : Boolean) : Tuple2[Entrypoint, String] = {

//     val startClassGeneration = System.currentTimeMillis // PROFILE

    val hardCodedClassModels : HardCodedClassModels = new HardCodedClassModels()
    if (firstSample.isInstanceOf[Tuple2[_, _]]) {
      CodeGenUtil.createHardCodedTuple2ClassModel(firstSample.asInstanceOf[Tuple2[_, _]],
          hardCodedClassModels, params.get(0))
    } else if (firstSample.isInstanceOf[DenseVector]) {
      CodeGenUtil.createHardCodedDenseVectorClassModel(hardCodedClassModels)
    } else if (firstSample.isInstanceOf[SparseVector]) {
      CodeGenUtil.createHardCodedSparseVectorClassModel(hardCodedClassModels)
    }

    createHardCodedClassModelsForSampleOutput(sampleOutput,
            hardCodedClassModels, params)

//     profPrint("HardCodedClassGeneration", startClassGeneration, threadId) // PROFILE
//     val startEntrypointGeneration = System.currentTimeMillis // PROFILE

    getEntrypointAndKernelHelper(hardCodedClassModels,
            lambda.asInstanceOf[java.lang.Object], classModel, methodDescriptor,
            params, dev_ctx, kernelDir, printKernel, false)
  }

  def getThreadID() : Int = {
    val threadNamePrefix = "Executor task launch worker-"
    val threadName = Thread.currentThread.getName
    if (!threadName.startsWith(threadNamePrefix)) {
        throw new RuntimeException("Unexpected thread name \"" + threadName + "\"")
    }
    Integer.parseInt(threadName.substring(threadNamePrefix.length))
  }

  def getInputBufferForSample[T : ClassTag](firstSample : T, N : Int,
      denseVectorTiling : Int, sparseVectorTiling : Int,
      primitiveArrayTiling : Int, entryPoint : Entrypoint,
      blockingCopies : Boolean, isMapAsync : Boolean) : InputBufferWrapper[T] = {
    if (isMapAsync) {
        new LambdaInputBufferWrapper(N, firstSample, entryPoint, blockingCopies)
    } else if (firstSample.isInstanceOf[Array[_]]) {
        val arr : Array[_] = firstSample.asInstanceOf[Array[_]]
        new PrimitiveArrayInputBufferWrapper[T](
                N * arr.length, N, primitiveArrayTiling, entryPoint,
                blockingCopies, firstSample)
    } else if (firstSample.isInstanceOf[Double]) {
      new PrimitiveInputBufferWrapper[Double](N, blockingCopies).asInstanceOf[PrimitiveInputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[Int]) {
      new PrimitiveInputBufferWrapper[Int](N, blockingCopies).asInstanceOf[PrimitiveInputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[Float]) {
      new PrimitiveInputBufferWrapper[Float](N, blockingCopies).asInstanceOf[PrimitiveInputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[Tuple2[_, _]]) {
      new Tuple2InputBufferWrapper(N,
              firstSample.asInstanceOf[Tuple2[_, _]],
              entryPoint, blockingCopies).asInstanceOf[InputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[DenseVector]) {
      new DenseVectorInputBufferWrapper(
              N * firstSample.asInstanceOf[DenseVector].size, N,
              denseVectorTiling, entryPoint, blockingCopies)
          .asInstanceOf[InputBufferWrapper[T]]
    } else if (firstSample.isInstanceOf[SparseVector]) {
      new SparseVectorInputBufferWrapper(
              N * firstSample.asInstanceOf[SparseVector].size, N,
              sparseVectorTiling, entryPoint, blockingCopies)
          .asInstanceOf[InputBufferWrapper[T]]
    } else {
      new ObjectInputBufferWrapper(N,
              firstSample.getClass.getName, entryPoint, blockingCopies)
    }
  }

  def tryCacheDenseVector(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nVectors : Int, tiling : Int,
      entryPoint : Entrypoint, persistent : Boolean) : Int = {

    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum + 1, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component, 3, persistent)) {
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.DENSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.DENSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
              c.getTotalStructSize * nVectors, persistent)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, argnum + 4, nVectors)
      // Tiling
      OpenCLBridge.setIntArg(ctx, argnum + 5, tiling)
      return 6
    } else {
      return -1
    }
  }

  def tryCacheSparseVector(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nVectors : Int, tiling : Int, entryPoint : Entrypoint,
      persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum + 1, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component, 4,
        persistent)) {
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.SPARSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.SPARSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
              c.getTotalStructSize * nVectors, persistent)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, argnum + 5, nVectors)
      // Tiling
      OpenCLBridge.setIntArg(ctx, argnum + 6, tiling)
      return 7
    } else {
      return -1
    }
  }

  def tryCacheObject(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, persistent : Boolean) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component,
        1, persistent)) {
      return 1
    } else {
      return -1
    }
  }

  def tryCacheTuple(ctx : Long, dev_ctx : Long, startArgnum : Int,
          cacheID : CLCacheID, nLoaded : Int, sample : Tuple2[_, _],
          denseVectorTiling : Int, sparseVectorTiling : Int,
          entryPoint : Entrypoint, persistent : Boolean) : Int = {
    val firstMemberClassName : String = sample._1.getClass.getName
    val secondMemberClassName : String = sample._2.getClass.getName
    var used = 0

    val firstMemberSize = entryPoint.getSizeOf(firstMemberClassName)
    val secondMemberSize = entryPoint.getSizeOf(secondMemberClassName)

    if (firstMemberSize > 0) {
        val cacheSuccess = tryCacheHelper(firstMemberClassName, ctx, dev_ctx,
                startArgnum, cacheID, nLoaded, denseVectorTiling,
                sparseVectorTiling, entryPoint, persistent)
        if (cacheSuccess == -1) {
          return -1
        }
        used = used + cacheSuccess
        cacheID.incrComponent(used)
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum)
        used = used + 1
    }

    if (secondMemberSize > 0) {
        val cacheSuccess = tryCacheHelper(secondMemberClassName, ctx, dev_ctx,
                startArgnum + used, cacheID, nLoaded, denseVectorTiling,
                sparseVectorTiling, entryPoint, persistent)
        if (cacheSuccess == -1) {
          OpenCLBridge.releaseAllPendingRegions(ctx)
          return -1
        }
        used = used + cacheSuccess
    } else {
        OpenCLBridge.setNullArrayArg(ctx, startArgnum + used)
        used = used + 1
    }

    val tuple2ClassModel : ClassModel =
      entryPoint.getHardCodedClassModels().getClassModelFor("scala.Tuple2",
          new ObjectMatcher(sample))
    OpenCLBridge.setArgUnitialized(ctx, dev_ctx, startArgnum + used,
            tuple2ClassModel.getTotalStructSize * nLoaded, persistent)
    return used + 1
  }

  def tryCacheHelper(desc : String, ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nLoaded : Int, denseVectorTiling : Int,
      sparseVectorTiling : Int, entryPoint : Entrypoint, persistent : Boolean) :
      Int = {
    desc match {
      case "I" | "F" | "D" => {
        if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum, cacheID.broadcast,
                    cacheID.rdd, cacheID.partition, cacheID.offset,
                    cacheID.component, 1, persistent)) {
          return 1
        } else {
          return -1
        }
      }
      case KernelWriter.DENSEVECTOR_CLASSNAME => {
        return tryCacheDenseVector(ctx, dev_ctx, argnum, cacheID, nLoaded,
                denseVectorTiling, entryPoint, persistent)
      }
      case KernelWriter.SPARSEVECTOR_CLASSNAME => {
        return tryCacheSparseVector(ctx, dev_ctx, argnum, cacheID, nLoaded,
                sparseVectorTiling, entryPoint, persistent)
      }
      case "scala.Tuple2" => {
        throw new UnsupportedOperationException()
      }
      case _ => {
        return tryCacheObject(ctx, dev_ctx, argnum, cacheID, persistent)
      }
    }
  }

  def getLabelForBufferCache[T, U](f : Any, firstSample : T, N : Int) : String = {
    var label : String = f.getClass.getName
    if (firstSample.isInstanceOf[Tuple2[_, _]]) {
      val sampledTuple = firstSample.asInstanceOf[Tuple2[_, _]]
      if (sampledTuple._1.isInstanceOf[DenseVector]) {
        label = label + "|" + sampledTuple._1.asInstanceOf[DenseVector].size
      } else if (sampledTuple._1.isInstanceOf[SparseVector]) {
        label = label + "|" + sampledTuple._1.asInstanceOf[SparseVector].size
      }

      if (sampledTuple._2.isInstanceOf[DenseVector]) {
        label = label + "|" + sampledTuple._2.asInstanceOf[DenseVector].size
      } else if (sampledTuple._2.isInstanceOf[SparseVector]) {
        label = label + "|" + sampledTuple._2.asInstanceOf[SparseVector].size
      }
    } else if (firstSample.isInstanceOf[DenseVector]) {
      label = label + "|" + firstSample.asInstanceOf[DenseVector].size
    } else if (firstSample.isInstanceOf[SparseVector]) {
      label = label + "|" + firstSample.asInstanceOf[SparseVector].size
    }
    label + "|" + N
  }

  def getElementVectorLengthHint(sample : Any) : Int = {
    if (sample.isInstanceOf[DenseVector]) {
      sample.asInstanceOf[DenseVector].size
    } else if (sample.isInstanceOf[SparseVector]) {
      sample.asInstanceOf[SparseVector].size
    } else {
      -1
    }
  }
}
