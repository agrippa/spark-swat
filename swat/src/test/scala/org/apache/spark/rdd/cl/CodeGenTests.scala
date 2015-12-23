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

  val syncTests : ArrayList[SyncCodeGenTest[_, _]] =
    new ArrayList[SyncCodeGenTest[_, _]]()
  syncTests.add(PrimitiveInputPrimitiveOutputTest)
  syncTests.add(PrimitiveInputObjectOutputTest)
  syncTests.add(ObjectInputObjectOutputTest)
  syncTests.add(ReferenceExternalArrayTest)
  syncTests.add(ReferenceExternalObjectArrayTest)
  syncTests.add(ReferenceExternalScalarTest)
  syncTests.add(ExternalFunctionTest)
  syncTests.add(Tuple2InputTest)
  syncTests.add(Tuple2ObjectInputTest)
  syncTests.add(Tuple2ObjectInputDirectTest)
  syncTests.add(Tuple2InputPassToFuncTest)
  syncTests.add(Tuple2ObjectInputPassToFuncTest)
  syncTests.add(Tuple2ObjectInputPassDirectlyToFuncTest)
  syncTests.add(Tuple2OutputTest)
  syncTests.add(Tuple2ObjectOutputTest)
  syncTests.add(Tuple2InputOutputTest)
  syncTests.add(KMeansTest)
  syncTests.add(DenseVectorInputTest)
  syncTests.add(SparseVectorInputTest)
  syncTests.add(SparseVectorAssignTest)
  syncTests.add(ArrayAllocTest)
  syncTests.add(DenseVectorOutputTest)
  syncTests.add(SparseVectorOutputTest)
  syncTests.add(PrimitiveArrayBroadcastTest)
  syncTests.add(DenseVectorBroadcastTest)
  syncTests.add(SparseVectorBroadcastTest)
  syncTests.add(Tuple2DenseInputTest)
  syncTests.add(ClassExternalFunctionTest)
  syncTests.add(Tuple2DenseOutputTest)
  syncTests.add(Tuple2BroadcastTest)
  syncTests.add(Tuple2ObjectBroadcastTest)
  syncTests.add(NestedTuple2OutputTest)
  syncTests.add(NestedTuple2OutputDenseTest)
  syncTests.add(PrimitiveArrayInputTest)

  val asyncTests : ArrayList[AsyncCodeGenTest[_]] =
    new ArrayList[AsyncCodeGenTest[_]]()
  asyncTests.add(AsyncMapTest)

  def verifyCodeGen(lambda : java.lang.Object, expectedKernel : String,
      expectedNumArguments : Int, testName : String, expectedException : String,
      test : CodeGenTest[_], devId : Int, isAsync : Boolean) {
    val classModel : ClassModel = ClassModel.createClassModel(lambda.getClass,
        null, new ShouldNotCallMatcher())
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    val params : LinkedList[ScalaArrayParameter] = new LinkedList[ScalaArrayParameter]
    if (!isAsync) {
      params.addAll(CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, expectedNumArguments))
    }
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

    test.complete(params)

    val hardCodedClassModels : HardCodedClassModels = test.init

    val dev_ctx : Long = OpenCLBridge.getActualDeviceContext(devId, 1, 1024, 0.2, true)
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
              entryPoint, params, isAsync)
      val openCL : String = writerAndKernel.kernel

      val ctx : Long = OpenCLBridge.createSwatContext(lambda.getClass.getName,
          openCL, dev_ctx, 0, entryPoint.requiresDoublePragma,
          entryPoint.requiresHeap, 1);

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
    var testName : String = null
    var devId : Int = 0

    var i = 0
    while (i < args.length) {
      if (args(i) == "-d") {
        devId = args(i + 1).toInt
        i += 1
      } else if (args(i) == "-t") {
        testName = args(i + 1)
        i += 1
      } else if (args(i) == "-h") {
        System.err.println("usage: scala CodeGenTests [-d devid] [-t testname]")
        System.exit(1)
      } else {
        System.err.println("Unknown command line argument \"" + args(i) + "\"")
        System.exit(1)
      }
      i += 1
    }

    System.setProperty("com.amd.aparapi.enable.NEW", "true");

    for (i <- 0 until syncTests.size) {
      val test : SyncCodeGenTest[_, _] = syncTests.get(i)
      if (testName == null || test.getClass.getSimpleName.equals(testName + "$")) {
        verifyCodeGen(test.getFunction, test.getExpectedKernel,
            test.getExpectedNumInputs, test.getClass.getSimpleName,
            test.getExpectedException, test, devId, false)
      }
    }

    for (i <- 0 until asyncTests.size) {
      val test : AsyncCodeGenTest[_] = asyncTests.get(i)
      if (testName == null || test.getClass.getSimpleName.equals(testName + "$")) {
        verifyCodeGen(test.getFunction, test.getExpectedKernel,
            test.getExpectedNumInputs, test.getClass.getSimpleName,
            test.getExpectedException, test, devId, true)
      }
    }
  }
}
