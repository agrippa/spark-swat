package org.apache.spark.rdd.cl

import java.util.LinkedList
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter.DIRECTION
import com.amd.aparapi.internal.model.Entrypoint

object CodeGenUtil {
  def isPrimitive(typeString : String) : Boolean = {
    return typeString.equals("I") || typeString.equals("D") || typeString.equals("F")
  }

  def getPrimitiveTypeForDescriptor(descString : String) : String = {
    if (descString.equals("I")) {
      return "int"
    } else if (descString.equals("D")) {
      return "double"
    } else if (descString.equals("F")) {
      return "float"
    } else {
      return null
    }
  }

  def getClassForDescriptor(descString : String) : Class[_] = {
    if (isPrimitive(descString)) {
      return null
    }

    var className : String = getTypeForDescriptor(descString)
    return Class.forName(className.trim)
  }

  def getTypeForDescriptor(descString : String) : String = {
    var primitive : String = getPrimitiveTypeForDescriptor(descString)
    if (primitive == null) {
      primitive = ClassModel.convert(descString, "", true)
    }
    primitive
  }

  def getParamObjsFromMethodDescriptor(descriptor : String,
      expectedNumParams : Int) : LinkedList[ScalaArrayParameter] = {
    val arguments : String = descriptor.substring(descriptor.indexOf('(') + 1,
        descriptor.lastIndexOf(')'))
    val argumentsArr : Array[String] = arguments.split(",")

    assert(argumentsArr.length == expectedNumParams)

    val params = new LinkedList[ScalaArrayParameter]()

    for (i <- 0 until argumentsArr.length) {
      val argumentDesc : String = argumentsArr(i)

      params.add(new ScalaArrayParameter(getTypeForDescriptor(argumentDesc),
            getClassForDescriptor(argumentDesc), "in" + i, DIRECTION.IN))
    }

    params
  }

  def getReturnObjsFromMethodDescriptor(descriptor : String) : ScalaArrayParameter = {
    val returnType : String = descriptor.substring(
        descriptor.lastIndexOf(')') + 1)
    new ScalaArrayParameter(getTypeForDescriptor(returnType),
        getClassForDescriptor(returnType), "out", DIRECTION.OUT)
  }

  def cleanClassName(className : String) : String = {
    if (className.length() == 1) {
      // Primitive descriptor
      return className
    } else if (className.equals("java.lang.Integer")) {
      return "I"
    } else if (className.equals("java.lang.Float")) {
      return "F"
    } else if (className.equals("java.lang.Double")) {
      return "D"
    } else {
      return "L" + className + ";"
    }
  }

  def createCodeGenConfig(dev_ctx : Long) : java.util.Map[String, String] = {
    assert(dev_ctx != -1L)
    val config : java.util.Map[String, String] = new java.util.HashMap[String, String]()

    config.put(Entrypoint.denseVectorTilingConfig, Integer.toString(
                DenseVectorInputBufferWrapperConfig.tiling))
    config.put(Entrypoint.sparseVectorTilingConfig, Integer.toString(
                SparseVectorInputBufferWrapperConfig.tiling))
    config.put(Entrypoint.clDevicePointerSize, Integer.toString(
                OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)))

    config
  }
}
