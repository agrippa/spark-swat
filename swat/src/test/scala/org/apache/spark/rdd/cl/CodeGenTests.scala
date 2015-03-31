package org.apache.spark.rdd.cl

import java.util.LinkedList
import java.util.ArrayList
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.cl.tests._
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter

object CodeGenTests {

  val tests : ArrayList[CodeGenTest[_, _]] = new ArrayList[CodeGenTest[_, _]]()
  tests.add(PrimitiveInputPrimitiveOutputTest)
  tests.add(PrimitiveInputObjectOutputTest)
  tests.add(ObjectInputObjectOutputTest)
  tests.add(ReferenceExternalArrayTest)
  tests.add(ReferenceExternalScalarTest)
  tests.add(ExternalFunction)
  tests.add(Tuple2InputTest)
  tests.add(Tuple2ObjectInputTest)
  tests.add(Tuple2ObjectInputDirectTest)
  tests.add(Tuple2InputPassToFuncTest)
  tests.add(Tuple2ObjectInputPassToFuncTest)
  tests.add(Tuple2ObjectInputPassDirectlyToFuncTest)
  tests.add(Tuple2OutputTest)
  tests.add(Tuple2ObjectOutputTest)

  def verifyCodeGen(lambda : java.lang.Object, expectedKernel : String,
      expectedNumArguments : Int, testName : String, test : CodeGenTest[_, _]) {
    val classModel : ClassModel = ClassModel.createClassModel(lambda.getClass)
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    val params : LinkedList[ScalaParameter] =
        CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, expectedNumArguments)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

    test.complete(params)

    val entryPoint : Entrypoint = classModel.getEntrypoint("apply", descriptor, lambda, params);

    val writerAndKernel : WriterAndKernel = KernelWriter.writeToString(entryPoint, params)
    val openCL : String = writerAndKernel.kernel

    val ctx : Long = OpenCLBridge.createContext(openCL,
        entryPoint.requiresDoublePragma, entryPoint.requiresHeap);

    if (!openCL.equals(expectedKernel)) {
      System.err.println(testName + " FAILED")
      System.err.println("Kernel mismatch, generated output in 'generated', correct output in 'correct'")
      System.err.println("Use 'vimdiff correct generated' to see the difference")

      Files.write(Paths.get("generated"), openCL.getBytes(StandardCharsets.UTF_8))
      Files.write(Paths.get("correct"), expectedKernel.getBytes(StandardCharsets.UTF_8))

      System.exit(1)
    } else {
      System.err.println(testName + " PASSED")
    }
  }

  def main(args : Array[String]) {
    System.setProperty("com.amd.aparapi.enable.NEW", "true");

    for (i <- 0 until tests.size) {
      ClassModel.hardCodedClassModels.clear

      val test : CodeGenTest[_, _] = tests.get(i)
      test.init
      verifyCodeGen(test.getFunction, test.getExpectedKernel,
          test.getExpectedNumInputs, test.getClass.getSimpleName, test)
    }
  }
}
