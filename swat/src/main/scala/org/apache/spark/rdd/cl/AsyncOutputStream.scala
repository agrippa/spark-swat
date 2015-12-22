package org.apache.spark.rdd.cl

import java.util.LinkedList

import scala.reflect.ClassTag

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.model.HardCodedClassModels.ShouldNotCallMatcher
import com.amd.aparapi.internal.writer.ScalaArrayParameter

class AsyncOutputStream[U: ClassTag](val multiOutput : Boolean,
    val dev_ctx : Long, val threadId : Int) {
  val tmpBuffer : java.util.LinkedList[Option[U]] = new java.util.LinkedList[Option[U]]

  var lambdaName : String = null
  var entryPoint : Entrypoint = null
  var openCL : String = null

  def spawn(l: () => U) {
    assert(l != null)

    val output : Option[U] = Some(l())

    val initializing : Boolean = (entryPoint == null)
    if (initializing) {
      lambdaName = l.getClass.getName

      val classModel : ClassModel = ClassModel.createClassModel(l.getClass, null,
          new ShouldNotCallMatcher())
      val method = classModel.getPrimitiveApplyMethod
      val descriptor : String = method.getDescriptor

      // 1 argument expected for maps
      val params : LinkedList[ScalaArrayParameter] = new LinkedList[ScalaArrayParameter]
      params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

      val entrypointAndKernel : Tuple2[Entrypoint, String] =
          RuntimeUtil.getEntrypointAndKernel[U](output.get.asInstanceOf[java.lang.Object],
          params, l, classModel, descriptor, dev_ctx, threadId,
          CLConfig.kernelDir, CLConfig.printKernel)
      entryPoint = entrypointAndKernel._1
      openCL = entrypointAndKernel._2
    } else {
      assert(lambdaName == l.getClass.getName)
    }

    tmpBuffer.synchronized {
      tmpBuffer.add(output)
      tmpBuffer.notify
    }

    if (!multiOutput) {
      throw new SuspendException
    }
  }

  def finish() {
    tmpBuffer.synchronized {
      tmpBuffer.add(None)
      tmpBuffer.notify
    }
  }

  def pop() : Option[U] = {
    var result : Option[U] = None

    tmpBuffer.synchronized {
      while (tmpBuffer.isEmpty) {
        tmpBuffer.wait
      }

      result = tmpBuffer.poll
    }

    result
  }
}
