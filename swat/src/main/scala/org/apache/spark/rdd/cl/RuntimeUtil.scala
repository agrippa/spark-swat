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
import com.amd.aparapi.internal.model.ClassModel.NameMatcher
import com.amd.aparapi.internal.model.Entrypoint

object RuntimeUtil {

//   def profPrint(lbl : String, startTime : Long, threadId : Int) { // PROFILE
//       System.err.println("SWAT PROF " + threadId + " " + lbl + " " + // PROFILE
//           (System.currentTimeMillis - startTime) + " ms") // PROFILE
//   } // PROFILE


  def getEntrypointAndKernel[T: ClassTag, U: ClassTag](firstSample : T,
      sampleOutput : java.lang.Object, params : LinkedList[ScalaArrayParameter],
      lambda : T => U, classModel : ClassModel, methodDescriptor : String,
      dev_ctx : Long, threadId : Int) : Tuple2[Entrypoint, String] = {
    var entryPoint : Entrypoint = null
    var openCL : String = null

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

//     profPrint("HardCodedClassGeneration", startClassGeneration, threadId) // PROFILE
//     val startEntrypointGeneration = System.currentTimeMillis // PROFILE

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
            entryPoint, params)
        openCL = writerAndKernel.kernel
//         System.err.println(openCL) // PROFILE
        EntrypointCache.kernelCache.put(entrypointKey, openCL)
      }

//       profPrint("KernelGeneration", startKernelGeneration, threadId) // PROFILE
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

  def tryCacheDenseVector(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nVectors : Int, entryPoint : Entrypoint) : Int = {

    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum + 1, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component, 3)) {
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.DENSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.DENSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
              c.getTotalStructSize * nVectors)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, argnum + 4, nVectors)
      return 5
    } else {
      return -1
    }
  }

  def tryCacheSparseVector(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nVectors : Int, entryPoint : Entrypoint) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum + 1, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component, 4)) {
      // Array of structs for each item
      val c : ClassModel = entryPoint.getModelFromObjectArrayFieldsClasses(
          KernelWriter.SPARSEVECTOR_CLASSNAME,
          new NameMatcher(KernelWriter.SPARSEVECTOR_CLASSNAME))
      OpenCLBridge.setArgUnitialized(ctx, dev_ctx, argnum,
              c.getTotalStructSize * nVectors)
      // Number of vectors
      OpenCLBridge.setIntArg(ctx, argnum + 5, nVectors)
      return 6
    } else {
      return -1
    }
  }

  def tryCacheObject(ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID) : Int = {
    if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum, cacheID.broadcast,
        cacheID.rdd, cacheID.partition, cacheID.offset, cacheID.component,
        1)) {
      return 1
    } else {
      return -1
    }
  }

  def tryCacheTuple(ctx : Long, dev_ctx : Long, startArgnum : Int,
          cacheID : CLCacheID, nLoaded : Int, sample : Tuple2[_, _],
          entryPoint : Entrypoint) : Int = {
    val firstMemberClassName : String = sample._1.getClass.getName
    val secondMemberClassName : String = sample._2.getClass.getName
    var used = 0

    val firstMemberSize = entryPoint.getSizeOf(firstMemberClassName)
    val secondMemberSize = entryPoint.getSizeOf(secondMemberClassName)

    if (firstMemberSize > 0) {
        val cacheSuccess = tryCacheHelper(firstMemberClassName, ctx, dev_ctx,
                startArgnum, cacheID, nLoaded, entryPoint)
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
                startArgnum + used, cacheID, nLoaded, entryPoint)
        if (cacheSuccess == -1) {
          OpenCLBridge.manuallyRelease(ctx, dev_ctx, startArgnum, used)
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
            tuple2ClassModel.getTotalStructSize * nLoaded)
    return used + 1
  }

  def tryCacheHelper(desc : String, ctx : Long, dev_ctx : Long, argnum : Int,
      cacheID : CLCacheID, nLoaded : Int, entryPoint : Entrypoint) : Int = {
    desc match {
      case "I" | "F" | "D" => {
        if (OpenCLBridge.tryCache(ctx, dev_ctx, argnum, cacheID.broadcast,
                    cacheID.rdd, cacheID.partition, cacheID.offset,
                    cacheID.component, 1)) {
          return 1
        } else {
          return -1
        }
      }
      case KernelWriter.DENSEVECTOR_CLASSNAME => {
        return tryCacheDenseVector(ctx, dev_ctx, argnum, cacheID, nLoaded,
                entryPoint)
      }
      case KernelWriter.SPARSEVECTOR_CLASSNAME => {
        return tryCacheSparseVector(ctx, dev_ctx, argnum, cacheID, nLoaded,
                entryPoint)
      }
      case "scala.Tuple2" => {
        throw new UnsupportedOperationException()
      }
      case _ => {
        return tryCacheObject(ctx, dev_ctx, argnum, cacheID)
      }
    }
  }
}
