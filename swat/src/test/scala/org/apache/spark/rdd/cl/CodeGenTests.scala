package org.apache.spark.rdd.cl

import java.util.ArrayList

import org.apache.spark.rdd.cl.tests.PrimitiveInputPrimitiveOutputTest
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel

object CodeGenTests {

  val tests : ArrayList[CodeGenTest] = new ArrayList[CodeGenTest]()
  tests.add(new PrimitiveInputPrimitiveOutputTest())

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
      System.err.println("Kernel mismatch")
      System.err.println("=======================================")
      System.err.println(openCL)
      System.err.println("=======================================")
      System.err.println(expectedKernel)
      System.err.println("=======================================")
      throw new RuntimeException;
    } else {
      System.err.println(testName + " PASSED")
    }
  }

  def main(args : Array[String]) {
    for (i <- 0 until tests.size) {
      val test : CodeGenTest = tests.get(i)
      verifyCodeGen(test, test.getExpectedKernel, test.getExpectedNumInputs, test.getClass.getSimpleName)
    }
  }
}
