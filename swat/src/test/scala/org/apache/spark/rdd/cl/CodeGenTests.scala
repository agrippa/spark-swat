package org.apache.spark.rdd.cl

import java.util.ArrayList
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.cl.tests._
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel

object CodeGenTests {

  val tests : ArrayList[CodeGenTest[_, _]] = new ArrayList[CodeGenTest[_, _]]()
  tests.add(PrimitiveInputPrimitiveOutputTest)
  tests.add(PrimitiveInputObjectOutputTest)
  tests.add(ObjectInputObjectOutputTest)
  tests.add(ReferenceExternalArrayTest)
  tests.add(ReferenceExternalScalarTest)
  tests.add(ExternalFunction)

  def verifyCodeGen(lambda : java.lang.Object, expectedKernel : String,
      expectedNumArguments : Int, testName : String) {
    val classModel : ClassModel = ClassModel.createClassModel(lambda.getClass)
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    val params = CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, expectedNumArguments)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

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
      val test : CodeGenTest[_, _] = tests.get(i)
      verifyCodeGen(test.getFunction, test.getExpectedKernel, test.getExpectedNumInputs, test.getClass.getSimpleName)
    }
  }
}
