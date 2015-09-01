package org.apache.spark.rdd.cl

import java.util.Map
import java.util.HashMap
import java.util.LinkedList
import java.util.ArrayList
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.cl.tests._
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.BlockWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel
import com.amd.aparapi.internal.writer.ScalaArrayParameter

object CodeGenTests {

  val testsPath : String = sys.env("SWAT_HOME") +
      "/swat/src/test/scala/org/apache/spark/rdd/cl/tests/"

  val tests : ArrayList[CodeGenTest[_, _]] = new ArrayList[CodeGenTest[_, _]]()
  tests.add(PrimitiveInputPrimitiveOutputTest)
  tests.add(PrimitiveInputObjectOutputTest)
  tests.add(ObjectInputObjectOutputTest)
  tests.add(ReferenceExternalArrayTest)
  tests.add(ReferenceExternalObjectArrayTest)
  tests.add(ReferenceExternalScalarTest)
  tests.add(ExternalFunctionTest)
  tests.add(Tuple2InputTest)
  tests.add(Tuple2ObjectInputTest)
  tests.add(Tuple2ObjectInputDirectTest)
  tests.add(Tuple2InputPassToFuncTest)
  tests.add(Tuple2ObjectInputPassToFuncTest)
  tests.add(Tuple2ObjectInputPassDirectlyToFuncTest)
  tests.add(Tuple2OutputTest)
  tests.add(Tuple2ObjectOutputTest)
  tests.add(Tuple2InputOutputTest)
  tests.add(KMeansTest)
  tests.add(DenseVectorInputTest)
  tests.add(SparseVectorInputTest)
  tests.add(SparseVectorAssignTest)
  tests.add(ArrayAllocTest)
  tests.add(DenseVectorOutputTest)
  tests.add(SparseVectorOutputTest)
  tests.add(PrimitiveArrayBroadcastTest)
  tests.add(DenseVectorBroadcastTest)
  tests.add(SparseVectorBroadcastTest)
  tests.add(Tuple2DenseInputTest)
  tests.add(ClassExternalFunctionTest)
  tests.add(Tuple2DenseOutputTest)
  tests.add(InternalParallelismTest)

  def verifyCodeGen(lambda : java.lang.Object, expectedKernel : String,
      expectedNumArguments : Int, testName : String, expectedException : String,
      test : CodeGenTest[_, _]) {
    val classModel : ClassModel = ClassModel.createClassModel(lambda.getClass,
        null, new ShouldNotCallMatcher())
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    val params : LinkedList[ScalaArrayParameter] =
        CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, expectedNumArguments)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

    test.complete(params)

    val hardCodedClassModels : HardCodedClassModels = test.init

    val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(0)
    val config = CodeGenUtil.createCodeGenConfig(dev_ctx)
    var gotExpectedException = false
    var entryPoint : Entrypoint = null;
    try {
      entryPoint = classModel.getEntrypoint("apply", descriptor,
          lambda, params, hardCodedClassModels, config)
    } catch {
      case e: Exception => {
        if (expectedException == null) {
          throw e
        } else if (!e.getMessage().equals(expectedException)) {
          throw new RuntimeException("Expected exception \"" +
                  expectedException + "\" but got \"" + e.getMessage() +
                  "\"")
        } else {
          gotExpectedException = true
        }
      }
    }

    if (expectedException != null && !gotExpectedException) {
        System.err.println(testName + " FAILED")
        System.err.println("Expected exception \"" + expectedException + "\"")
        System.exit(1)
    }

    if (expectedException == null) {
      val writerAndKernel : WriterAndKernel = KernelWriter.writeToString(
              entryPoint, params)
      val openCL : String = writerAndKernel.kernel

      val ctx : Long = OpenCLBridge.createSwatContext(lambda.getClass.getName,
          openCL, dev_ctx, 0, entryPoint.requiresDoublePragma,
          entryPoint.requiresHeap);

      Files.write(Paths.get("generated"), openCL.getBytes(StandardCharsets.UTF_8))
      Files.write(Paths.get("correct"), expectedKernel.getBytes(StandardCharsets.UTF_8))

      if (!openCL.equals(expectedKernel)) {
        System.err.println(testName + " FAILED")
        System.err.println("Kernel mismatch, generated output in 'generated', correct output in 'correct'")
        System.err.println("Use 'vimdiff correct generated' to see the difference")

        System.exit(1)
      }
    }

    System.err.println(testName + " PASSED")
  }

  def main(args : Array[String]) {
    val testName : String = if (args.length == 1) args(0) else null
    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    for (i <- 0 until tests.size) {
      val test : CodeGenTest[_, _] = tests.get(i)
      if (testName == null || test.getClass.getSimpleName.equals(testName + "$")) {
        verifyCodeGen(test.getFunction, test.getExpectedKernel,
            test.getExpectedNumInputs, test.getClass.getSimpleName,
            test.getExpectedException, test)
      }
    }
  }
}
