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
  syncTests.add(PrimitiveArrayInputTest)
  syncTests.add(ArrayOutputTest)
  syncTests.add(ByteArrayInputTest)
  syncTests.add(ExtensionTest)
  syncTests.add(ASPLOSAES)
  syncTests.add(ASPLOSBlackScholes)
  syncTests.add(ASPLOSPageRank)

  val asyncTests : ArrayList[AsyncCodeGenTest[_]] =
    new ArrayList[AsyncCodeGenTest[_]]()
  asyncTests.add(AsyncMapTest)
  asyncTests.add(AsyncPrimitiveArrayInputTest)
  asyncTests.add(AsyncArrayOutputTest)
  asyncTests.add(AsyncByteArrayInputTest)

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
        val expectedOutput : String = try {
            test.getExpectedKernel
        } catch {
            case m: MissingTestException => {
                System.err.println(test.getClass.getSimpleName + " FAILED")
                System.err.println("Missing expected kernel output at " + m.getMessage)
                System.exit(1)
                ""
            }
        }
        verifyCodeGen(test.getFunction, expectedOutput,
            test.getExpectedNumInputs, test.getClass.getSimpleName,
            test.getExpectedException, test, devId, false)
      }
    }

    for (i <- 0 until asyncTests.size) {
      val test : AsyncCodeGenTest[_] = asyncTests.get(i)
      if (testName == null || test.getClass.getSimpleName.equals(testName + "$")) {
        val expectedOutput : String = try {
            test.getExpectedKernel
        } catch {
            case m: MissingTestException => {
                System.err.println(test.getClass.getSimpleName + " FAILED")
                System.err.println("Missing expected kernel output at " + m.getMessage)
                System.exit(1)
                ""
            }
        }
        verifyCodeGen(test.getFunction, expectedOutput,
            test.getExpectedNumInputs, test.getClass.getSimpleName,
            test.getExpectedException, test, devId, true)
      }
    }
  }
}
