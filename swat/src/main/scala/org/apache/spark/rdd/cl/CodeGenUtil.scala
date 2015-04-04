package org.apache.spark.rdd.cl

import java.util.LinkedList
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter.DIRECTION

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
      expectedNumParams : Int) : LinkedList[ScalaParameter] = {
    val arguments : String = descriptor.substring(descriptor.indexOf('(') + 1,
        descriptor.lastIndexOf(')'))
    val argumentsArr : Array[String] = arguments.split(",")

    assert(argumentsArr.length == expectedNumParams)

    val params = new LinkedList[ScalaParameter]()

    for (i <- 0 until argumentsArr.length) {
      val argumentDesc : String = argumentsArr(i)

      params.add(new ScalaParameter(getTypeForDescriptor(argumentDesc),
            getClassForDescriptor(argumentDesc), "in" + i, DIRECTION.IN))
    }

    params
  }

  def getReturnObjsFromMethodDescriptor(descriptor : String) : ScalaParameter = {
    val returnType : String = descriptor.substring(
        descriptor.lastIndexOf(')') + 1)
    new ScalaParameter(getTypeForDescriptor(returnType),
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
      return "F"
    } else {
      return "L" + className + ";"
    }
  }
}
