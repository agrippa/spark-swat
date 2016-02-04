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

import java.util.LinkedList
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.SparseVectorClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.writer.ScalaParameter
import com.amd.aparapi.internal.writer.ScalaParameter.DIRECTION
import com.amd.aparapi.internal.model.Entrypoint

object CodeGenUtil {
  def isPrimitive(typeString : String) : Boolean = {
    return typeString.equals("I") || typeString.equals("D") || typeString.equals("F") || typeString.equals("B")
  }

  def isPrimitiveArray(typeString : String) : Boolean = {
    return typeString.startsWith("[") && isPrimitive(typeString.substring(1))
  }

  def getPrimitiveTypeForDescriptor(descString : String) : String = {
    if (descString.equals("I")) {
      return "int"
    } else if (descString.equals("D")) {
      return "double"
    } else if (descString.equals("F")) {
      return "float"
    } else if (descString.equals("B")) {
      return "char"
    } else {
      return null
    }
  }

  def getClassForDescriptor(descString : String) : Class[_] = {
    if (isPrimitive(descString)) {
      return null
    } else if (isPrimitiveArray(descString)) {
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

      params.add(ScalaArrayParameter.createArrayParameterFor(
            getTypeForDescriptor(argumentDesc),
            getClassForDescriptor(argumentDesc), "in" + i, DIRECTION.IN))
    }

    params
  }

  def getReturnObjsFromMethodDescriptor(descriptor : String) : ScalaArrayParameter = {
    val returnType : String = descriptor.substring(
        descriptor.lastIndexOf(')') + 1)
    ScalaArrayParameter.createArrayParameterFor(getTypeForDescriptor(returnType),
        getClassForDescriptor(returnType), "out", DIRECTION.OUT)
  }

  def cleanClassName(className : String, objectMangling : Boolean = true) : String = {
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
      if (objectMangling) {
        return "L" + className + ";"
      } else {
        return className
      }
    }
  }

  def createCodeGenConfig(dev_ctx : Long) : java.util.Map[String, String] = {
    assert(dev_ctx != -1L)
    val config : java.util.Map[String, String] = new java.util.HashMap[String, String]()

    config.put(Entrypoint.clDevicePointerSize, Integer.toString(
                OpenCLBridge.getDevicePointerSizeInBytes(dev_ctx)))

    config
  }

  def createHardCodedDenseVectorClassModel(hardCodedClassModels : HardCodedClassModels) {
    val denseVectorClassModel : DenseVectorClassModel = DenseVectorClassModel.create()
    hardCodedClassModels.addClassModelFor(
            Class.forName("org.apache.spark.mllib.linalg.DenseVector"),
            denseVectorClassModel)
  }
  
  def createHardCodedSparseVectorClassModel(hardCodedClassModels : HardCodedClassModels) {
    val sparseVectorClassModel : SparseVectorClassModel = SparseVectorClassModel.create()
    hardCodedClassModels.addClassModelFor(
            Class.forName("org.apache.spark.mllib.linalg.SparseVector"),
            sparseVectorClassModel)
  }
  
  def createHardCodedTuple2ClassModel(obj : Tuple2[_, _],
      hardCodedClassModels : HardCodedClassModels,
      param : ScalaArrayParameter) {
    val inputClassType1 = obj._1.getClass
    val inputClassType2 = obj._2.getClass
  
    val inputClassType1Name = CodeGenUtil.cleanClassName(
        inputClassType1.getName)
    val inputClassType2Name = CodeGenUtil.cleanClassName(
        inputClassType2.getName)
  
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, param.getDir != DIRECTION.IN)
    hardCodedClassModels.addClassModelFor(Class.forName("scala.Tuple2"), tuple2ClassModel)
  
    param.addTypeParameter(inputClassType1Name,
        !CodeGenUtil.isPrimitive(inputClassType1Name))
    param.addTypeParameter(inputClassType2Name,
        !CodeGenUtil.isPrimitive(inputClassType2Name))
  }
}
