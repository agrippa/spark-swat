package org.apache.spark.rdd.cl

import java.util.Map
import java.util.HashMap
import java.util.LinkedList

import com.amd.aparapi.internal.util.UnsafeWrapper
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.writer.BlockWriter
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel

object SerializationPerfTests {
  def testTuple2IntIntInputBuffer(N : Int, chunking : Int) {
    val lambda = new Function[(Int, Int), Int] {
      override def apply(in : (Int, Int)) : Int = {
        in._1 + in._2
      }
    }
    val classModel : ClassModel = ClassModel.createClassModel(lambda.getClass,
        null, new ShouldNotCallMatcher())
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    val params : LinkedList[ScalaArrayParameter] =
        CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, 1)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))
    params.get(0).addTypeParameter("I", false)
    params.get(0).addTypeParameter("I", false)

    val hardCodedClassModels = new HardCodedClassModels()
    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("I")
    val inputTuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, false)
    hardCodedClassModels.addClassModelFor(classOf[Tuple2[_, _]], inputTuple2ClassModel)

    val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(0)
    val config = CodeGenUtil.createCodeGenConfig(dev_ctx)
    val entryPoint : Entrypoint = classModel.getEntrypoint("apply", descriptor,
        lambda, params, hardCodedClassModels, config)

    val writerAndKernel : WriterAndKernel = KernelWriter.writeToString(
            entryPoint, params)
    val openCL : String = writerAndKernel.kernel

    val acc : InputBufferWrapper[Tuple2[java.lang.Integer, java.lang.Integer]] =
        new Tuple2InputBufferWrapper(chunking, (0, 0), entryPoint)

    val ctx : Long = OpenCLBridge.createSwatContext(lambda.getClass.getName,
        openCL, dev_ctx, 0, entryPoint.requiresDoublePragma,
        entryPoint.requiresHeap);

    val startTime = System.currentTimeMillis
    for (i <- 0 until N) {
        if (!acc.hasSpace) {
            acc.copyToDevice(0, ctx, dev_ctx, -1, -1, -1)
        }
        acc.append((i, i))
    }
    acc.flush
    val endTime = System.currentTimeMillis
    System.err.println((endTime - startTime) + " ms")
  }

  def main(args : Array[String]) {
    val N : Int = if (args.length > 0) args(0).toInt else 1024 * 1024
    val chunking : Int = if (args.length > 1) args(1).toInt else 128

    testTuple2IntIntInputBuffer(N, chunking)
  }
}
